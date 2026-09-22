#!/usr/bin/env bash
# ==============================================================================
# build_android_worker.sh — Production Cross-Compilation Script for Android
#
# Targets: aarch64-linux-android (ARM64-v8a)
# Compatible with Android API 24+ (Android 7.0 Nougat through Android 15+)
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

TARGET="aarch64-linux-android"
API_LEVEL=24
BUILD_PROFILE="debug"
CHECK_ONLY=false
DRY_RUN=false
CARGO_FLAGS=""

usage() {
    cat <<EOF
${BOLD}Usage:${NC} $(basename "$0") [OPTIONS]

${BOLD}Options:${NC}
  --check               Verify host cross-compilation prerequisites (Cargo, target, NDK) and exit
  --dry-run             Print the resolved toolchain paths and cargo build command without compiling
  --release             Build in release mode (optimized, smaller binary)
  --target <TRIPLE>     Cross-compilation target (default: aarch64-linux-android)
  --api-level <N>       Android API level (default: 24)
  --features <LIST>     Cargo feature flags to pass
  -h, --help            Show this help message

${BOLD}Environment Variables:${NC}
  ANDROID_NDK_HOME      Path to Android NDK root directory (e.g. /path/to/ndk/25.x.x)
  ANDROID_HOME          Path to Android SDK directory
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
        --target)
            TARGET="$2"
            shift 2
            ;;
        --api-level)
            API_LEVEL="$2"
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
            usage
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log_info "Verifying host cross-compilation environment..."

# 1. Verify Cargo & Rust installation
if ! command -v cargo &>/dev/null; then
    log_err "Cargo is not installed or not in PATH."
    exit 1
fi
log_ok "Found Cargo: $(cargo --version)"

# 2. Check Rust target availability
if command -v rustup &>/dev/null; then
    if rustup target list | grep -q "${TARGET} (installed)"; then
        log_ok "Rust target '${TARGET}' is installed."
    else
        log_warn "Rust target '${TARGET}' is not currently installed."
        if [[ "$CHECK_ONLY" == false && "$DRY_RUN" == false ]]; then
            log_info "Attempting to install target via 'rustup target add ${TARGET}'..."
            rustup target add "${TARGET}" || log_warn "Failed to add target automatically; please run 'rustup target add ${TARGET}'."
        else
            log_info "To install run: rustup target add ${TARGET}"
        fi
    fi
else
    log_warn "rustup not found in PATH; skipping target auto-check."
fi

# 3. Locate Android NDK
NDK_DIR=""
if [[ -n "${ANDROID_NDK_HOME:-}" && -d "${ANDROID_NDK_HOME}" ]]; then
    NDK_DIR="${ANDROID_NDK_HOME}"
elif [[ -n "${ANDROID_NDK_ROOT:-}" && -d "${ANDROID_NDK_ROOT}" ]]; then
    NDK_DIR="${ANDROID_NDK_ROOT}"
elif [[ -n "${ANDROID_HOME:-}" && -d "${ANDROID_HOME}/ndk" ]]; then
    LATEST_NDK=$(ls -1d "${ANDROID_HOME}/ndk/"* 2>/dev/null | sort -V | tail -n1 || true)
    if [[ -n "$LATEST_NDK" && -d "$LATEST_NDK" ]]; then
        NDK_DIR="$LATEST_NDK"
    fi
elif [[ "$OSTYPE" == "darwin"* && -d "$HOME/Library/Android/sdk/ndk" ]]; then
    LATEST_NDK=$(ls -1d "$HOME/Library/Android/sdk/ndk/"* 2>/dev/null | sort -V | tail -n1 || true)
    if [[ -n "$LATEST_NDK" && -d "$LATEST_NDK" ]]; then
        NDK_DIR="$LATEST_NDK"
    fi
elif [[ -d "$HOME/Android/Sdk/ndk" ]]; then
    LATEST_NDK=$(ls -1d "$HOME/Android/Sdk/ndk/"* 2>/dev/null | sort -V | tail -n1 || true)
    if [[ -n "$LATEST_NDK" && -d "$LATEST_NDK" ]]; then
        NDK_DIR="$LATEST_NDK"
    fi
elif [[ -d "/opt/homebrew/share/android-ndk" ]]; then
    NDK_DIR="/opt/homebrew/share/android-ndk"
fi

# Detect host OS tag
HOST_TAG="linux-x86_64"
if [[ "$OSTYPE" == "darwin"* ]]; then
    HOST_TAG="darwin-x86_64"
fi

if [[ -n "$NDK_DIR" ]]; then
    log_ok "Found Android NDK at: ${NDK_DIR}"
else
    log_warn "Android NDK not located in standard paths."
    log_info "Note: Set ANDROID_NDK_HOME=/path/to/android-ndk to compile for actual Android devices."
fi

# If check-only mode was requested, report success and exit 0
if [[ "$CHECK_ONLY" == true ]]; then
    log_ok "Cross-compilation environment verification check completed successfully."
    exit 0
fi

# Configure toolchain variables if NDK is present
if [[ -n "$NDK_DIR" ]]; then
    TOOLCHAIN="${NDK_DIR}/toolchains/llvm/prebuilt/${HOST_TAG}"
    CLANG_BIN="${TOOLCHAIN}/bin/aarch64-linux-android${API_LEVEL}-clang"
    AR_BIN="${TOOLCHAIN}/bin/llvm-ar"

    if [[ -f "$CLANG_BIN" ]]; then
        export CC_aarch64_linux_android="${CLANG_BIN}"
        export AR_aarch64_linux_android="${AR_BIN}"
        export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${CLANG_BIN}"
        log_ok "Configured Clang linker: ${CLANG_BIN}"
    fi
fi

PROFILE_FLAG=""
if [[ "$BUILD_PROFILE" == "release" ]]; then
    PROFILE_FLAG="--release"
fi

BUILD_CMD="cargo build --target ${TARGET} --bin rusty-grid ${PROFILE_FLAG} ${CARGO_FLAGS}"

if [[ "$DRY_RUN" == true ]]; then
    log_ok "[DRY-RUN] Execution plan confirmed."
    echo "Command: ${BUILD_CMD}"
    exit 0
fi

# Execute build
if [[ -z "$NDK_DIR" ]]; then
    log_err "Android NDK is required for native linking."
    log_err "Please set ANDROID_NDK_HOME or run with --dry-run."
    exit 1
fi

log_info "Executing: ${BUILD_CMD}"
eval "${BUILD_CMD}"

OUTPUT_BIN="${SCRIPT_DIR}/target/${TARGET}/${BUILD_PROFILE}/rusty-grid"
if [[ -f "$OUTPUT_BIN" ]]; then
    BIN_SIZE=$(ls -lh "$OUTPUT_BIN" | awk '{print $5}')
    log_ok "Android Worker binary built successfully!"
    echo "=================================================================="
    echo "Binary: ${OUTPUT_BIN} (${BIN_SIZE})"
    echo ""
    echo "Deployment to Android device:"
    echo "  1. Push binary: adb push ${OUTPUT_BIN} /data/local/tmp/rusty-grid"
    echo "  2. Make executable: adb shell chmod +x /data/local/tmp/rusty-grid"
    echo "  3. Run worker: adb shell /data/local/tmp/rusty-grid worker --master <MASTER_IP>"
    echo "=================================================================="
else
    log_err "Expected output binary not found at ${OUTPUT_BIN}"
    exit 1
fi
