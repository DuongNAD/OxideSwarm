#!/usr/bin/env bash
# ==============================================================================
# build_windows_worker.sh — Production Cross-Compilation Script for Windows
#
# Primary Target: x86_64-pc-windows-gnu (Windows 64-bit .exe)
# Cross-compiles from macOS (or Linux) using mingw-w64 toolchain
# ==============================================================================

set -euo pipefail

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

TARGET="x86_64-pc-windows-gnu"
BUILD_PROFILE="release"
CHECK_ONLY=false
DRY_RUN=false
CARGO_FLAGS=""

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --check               Verify host cross-compilation prerequisites (Cargo, rustup, target, mingw-w64) and exit
  --dry-run             Print the resolved toolchain paths and cargo build command without compiling
  --release             Build in release mode (optimized, default)
  --debug               Build in debug mode (unoptimized, faster compile)
  --target <TRIPLE>     Cross-compilation target (default: x86_64-pc-windows-gnu)
  --features <LIST>     Cargo feature flags to pass
  -h, --help            Show this help message

Environment Variables:
  CC_x86_64_pc_windows_gnu                  Custom C compiler path
  CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER Custom linker binary path
  MINGW_PREFIX                              Custom directory prefix containing mingw-w64 bin/
EOF
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)
            CHECK_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --release)
            BUILD_PROFILE="release"
            shift
            ;;
        --debug)
            BUILD_PROFILE="debug"
            shift
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --features)
            CARGO_FLAGS="${CARGO_FLAGS} --features $2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_err "Unknown argument: $1"
            echo "Use --help to view available options." >&2
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log_info "Verifying host cross-compilation environment for target '${TARGET}'..."

# 1. Verify Cargo & Rust installation
CARGO_AVAILABLE=false
if command -v cargo &>/dev/null; then
    CARGO_AVAILABLE=true
    log_ok "Found Cargo: $(cargo --version)"
else
    log_warn "Cargo is not installed or not in PATH."
    log_info "Setup guidance: Install Rust and Cargo via https://rustup.rs"
    if [[ "$CHECK_ONLY" == false && "$DRY_RUN" == false ]]; then
        log_err "Cargo is required for building."
        exit 1
    fi
fi

# 2. Check Rust target availability
RUSTUP_AVAILABLE=false
TARGET_INSTALLED=false
if command -v rustup &>/dev/null; then
    RUSTUP_AVAILABLE=true
    log_ok "Found rustup: $(rustup --version 2>/dev/null | head -n1)"
    if rustup target list 2>/dev/null | grep -q "${TARGET} (installed)"; then
        TARGET_INSTALLED=true
        log_ok "Rust target '${TARGET}' is installed."
    else
        log_warn "Rust target '${TARGET}' is not currently installed."
        if [[ "$CHECK_ONLY" == false && "$DRY_RUN" == false ]]; then
            log_info "Attempting to install target via 'rustup target add ${TARGET}'..."
            if rustup target add "${TARGET}"; then
                TARGET_INSTALLED=true
                log_ok "Successfully installed target '${TARGET}'."
            else
                log_warn "Failed to add target automatically; please run 'rustup target add ${TARGET}' manually."
            fi
        else
            log_info "Setup guidance: Run 'rustup target add ${TARGET}' to install the target."
        fi
    fi
else
    log_warn "rustup not found in PATH; skipping target auto-check."
    log_info "Setup guidance: Install rustup from https://rustup.rs for automatic target management."
fi

# 3. Locate mingw-w64 linker toolchain (x86_64-w64-mingw32-gcc)
MINGW_GCC=""

if [[ -n "${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:-}" ]]; then
    if [[ -x "${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER}" ]] || command -v "${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER}" &>/dev/null; then
        MINGW_GCC="${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER}"
    fi
elif [[ -n "${CC_x86_64_pc_windows_gnu:-}" ]]; then
    if [[ -x "${CC_x86_64_pc_windows_gnu}" ]] || command -v "${CC_x86_64_pc_windows_gnu}" &>/dev/null; then
        MINGW_GCC="${CC_x86_64_pc_windows_gnu}"
    fi
elif [[ -n "${MINGW_PREFIX:-}" && -x "${MINGW_PREFIX}/bin/x86_64-w64-mingw32-gcc" ]]; then
    MINGW_GCC="${MINGW_PREFIX}/bin/x86_64-w64-mingw32-gcc"
elif command -v x86_64-w64-mingw32-gcc &>/dev/null; then
    MINGW_GCC="$(command -v x86_64-w64-mingw32-gcc)"
elif [[ -x "/opt/homebrew/bin/x86_64-w64-mingw32-gcc" ]]; then
    MINGW_GCC="/opt/homebrew/bin/x86_64-w64-mingw32-gcc"
elif [[ -x "/usr/local/bin/x86_64-w64-mingw32-gcc" ]]; then
    MINGW_GCC="/usr/local/bin/x86_64-w64-mingw32-gcc"
elif [[ -x "/usr/bin/x86_64-w64-mingw32-gcc" ]]; then
    MINGW_GCC="/usr/bin/x86_64-w64-mingw32-gcc"
elif [[ -x "/opt/local/bin/x86_64-w64-mingw32-gcc" ]]; then
    MINGW_GCC="/opt/local/bin/x86_64-w64-mingw32-gcc"
fi

if [[ -n "$MINGW_GCC" ]]; then
    log_ok "Found mingw-w64 linker toolchain: ${MINGW_GCC}"
else
    log_warn "mingw-w64 linker toolchain (x86_64-w64-mingw32-gcc) not located in standard paths."
    log_info "Setup guidance to install mingw-w64:"
    log_info "  - macOS (Homebrew): brew install mingw-w64"
    log_info "  - macOS (MacPorts): sudo port install mingw-w64"
    log_info "  - Ubuntu/Debian:    sudo apt-get install gcc-mingw-w64-x86-64"
    log_info "  - Fedora/RHEL:      sudo dnf install mingw64-gcc"
    log_info "  - Arch Linux:       sudo pacman -S mingw-w64-gcc"
fi

# If check-only mode was requested, report diagnostic status and exit 0
if [[ "$CHECK_ONLY" == true ]]; then
    echo ""
    log_ok "Diagnostic status summary:"
    echo "  - Cargo:             $([ "$CARGO_AVAILABLE" = true ] && echo "FOUND" || echo "MISSING")"
    echo "  - rustup:            $([ "$RUSTUP_AVAILABLE" = true ] && echo "FOUND" || echo "MISSING")"
    echo "  - Target ($TARGET): $([ "$TARGET_INSTALLED" = true ] && echo "INSTALLED" || echo "MISSING")"
    echo "  - MinGW linker:      $([ -n "$MINGW_GCC" ] && echo "FOUND ($MINGW_GCC)" || echo "MISSING (see setup guidance above)")"
    echo ""
    log_ok "Cross-compilation environment verification check completed successfully."
    exit 0
fi

RESOLVED_LINKER="${MINGW_GCC:-x86_64-w64-mingw32-gcc}"
PLANNED_CC="${CC_x86_64_pc_windows_gnu:-$RESOLVED_LINKER}"
PLANNED_LINKER="${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:-$RESOLVED_LINKER}"

# Configure toolchain variables if linker is present
if [[ -n "$MINGW_GCC" ]]; then
    export CC_x86_64_pc_windows_gnu="${PLANNED_CC}"
    export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="${PLANNED_LINKER}"
    log_ok "Configured MinGW linker environment variables."
fi

PROFILE_FLAG=""
if [[ "$BUILD_PROFILE" == "release" ]]; then
    PROFILE_FLAG="--release"
fi

BUILD_CMD="cargo build --target ${TARGET} --bin rusty-grid"
if [[ -n "$PROFILE_FLAG" ]]; then
    BUILD_CMD="${BUILD_CMD} ${PROFILE_FLAG}"
fi
if [[ -n "$CARGO_FLAGS" ]]; then
    BUILD_CMD="${BUILD_CMD}${CARGO_FLAGS}"
fi

if [[ "$DRY_RUN" == true ]]; then
    echo ""
    log_ok "[DRY-RUN] Execution plan confirmed."
    echo "Planned Environment Variables:"
    echo "  CC_x86_64_pc_windows_gnu=${PLANNED_CC}"
    echo "  CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=${PLANNED_LINKER}"
    echo "Target: ${TARGET}"
    echo "Profile: ${BUILD_PROFILE}"
    echo "Command: ${BUILD_CMD}"
    exit 0
fi

# Execute build
if [[ -z "$MINGW_GCC" ]] && ! command -v "${CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER:-}" &>/dev/null && ! command -v "${CC_x86_64_pc_windows_gnu:-}" &>/dev/null; then
    log_err "mingw-w64 cross-linker (x86_64-w64-mingw32-gcc) is required for Windows cross-compilation."
    log_err "Please install it via 'brew install mingw-w64' (macOS) or 'apt-get install gcc-mingw-w64-x86-64' (Linux), or run with --dry-run."
    exit 1
fi

log_info "Executing: ${BUILD_CMD}"
eval "${BUILD_CMD}"

OUTPUT_BIN="${SCRIPT_DIR}/target/${TARGET}/${BUILD_PROFILE}/rusty-grid.exe"
if [[ -f "$OUTPUT_BIN" ]]; then
    BIN_SIZE=$(ls -lh "$OUTPUT_BIN" | awk '{print $5}')
    log_ok "Windows Worker binary built successfully!"
    echo "=================================================================="
    echo "Binary: ${OUTPUT_BIN} (${BIN_SIZE})"
    echo ""
    echo "Deployment to Windows machine:"
    echo "  1. Transfer binary to Windows host (e.g. scp, network share, USB):"
    echo "     scp ${OUTPUT_BIN} <USER>@<WINDOWS_HOST>:C:/rusty-grid/rusty-grid.exe"
    echo "  2. Run worker on Windows (PowerShell or cmd.exe):"
    echo "     .\\rusty-grid.exe worker --master <MASTER_IP>"
    echo "=================================================================="
else
    log_err "Expected output binary not found at ${OUTPUT_BIN}"
    exit 1
fi
