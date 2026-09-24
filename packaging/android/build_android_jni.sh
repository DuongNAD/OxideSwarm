#!/usr/bin/env bash
# ==============================================================================
# build_android_jni.sh — Automated JNI Cross-Compilation Script for OxideSwarm
#
# Compiles: liboxideworker.so (crates/android_bridge)
# Target:   aarch64-linux-android (ARM64-v8a)
# Output:   packaging/android/app/app/src/main/jniLibs/arm64-v8a/liboxideworker.so
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

get_abi_for_target() {
    case "$1" in
        aarch64-linux-android) echo "arm64-v8a" ;;
        armv7-linux-androideabi) echo "armeabi-v7a" ;;
        x86_64-linux-android) echo "x86_64" ;;
        i686-linux-android) echo "x86" ;;
        riscv64-linux-android) echo "riscv64" ;;
        *) echo "$1" ;;
    esac
}

get_clang_triple_for_target() {
    case "$1" in
        armv7-linux-androideabi) echo "armv7a-linux-androideabi" ;;
        *) echo "$1" ;;
    esac
}

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
  --dry-run             Print toolchain paths and cargo build command without compiling
  --release             Build in release mode (heavily optimized cdylib)
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
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${WORKSPACE_ROOT}"

log_info "Verifying host Android JNI cross-compilation environment..."

# 1. Verify Cargo & Rust installation
if ! command -v cargo &>/dev/null; then
    log_err "Cargo is not installed or not in PATH."
    exit 1
fi
log_ok "Found Cargo: $(cargo --version)"

# 2. Check Rust target availability
TARGET_INSTALLED=false
if command -v rustup &>/dev/null; then
    if rustup target list | grep -q "${TARGET} (installed)"; then
        log_ok "Rust target '${TARGET}' is installed."
        TARGET_INSTALLED=true
    else
        log_warn "Rust target '${TARGET}' is not currently installed."
        if [[ "$CHECK_ONLY" == false && "$DRY_RUN" == false ]]; then
            log_info "Attempting to install target via 'rustup target add ${TARGET}'..."
            if rustup target add "${TARGET}"; then
                TARGET_INSTALLED=true
            else
                log_warn "Failed to add target automatically; please run 'rustup target add ${TARGET}'."
            fi
        else
            log_info "To install run: rustup target add ${TARGET}"
        fi
    fi
else
    log_warn "rustup not found in PATH; skipping target auto-check."
    TARGET_INSTALLED=true
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
elif [[ -d "${LOCALAPPDATA:-}/Android/Sdk/ndk" ]]; then
    LATEST_NDK=$(ls -1d "${LOCALAPPDATA}/Android/Sdk/ndk/"* 2>/dev/null | sort -V | tail -n1 || true)
    if [[ -n "$LATEST_NDK" && -d "$LATEST_NDK" ]]; then
        NDK_DIR="$LATEST_NDK"
    fi
fi

# Detect host OS tag for NDK toolchain
HOST_TAG="linux-x86_64"
if [[ "$OSTYPE" == "darwin"* ]]; then
    HOST_TAG="darwin-x86_64"
elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win32"* ]]; then
    HOST_TAG="windows-x86_64"
fi

if [[ -n "$NDK_DIR" ]]; then
    log_ok "Found Android NDK at: ${NDK_DIR}"
else
    log_warn "Android NDK not located in standard paths."
    log_info "Note: Set ANDROID_NDK_HOME=/path/to/android-ndk to compile for actual Android devices."
fi

# If check-only mode was requested, report status and exit
if [[ "$CHECK_ONLY" == true ]]; then
    CHECK_SUCCESS=true
    if [[ -z "$NDK_DIR" ]]; then
        log_err "Prerequisite check failed: Android NDK not found in standard paths."
        log_info "Remediation: Set ANDROID_NDK_HOME=/path/to/android-ndk"
        CHECK_SUCCESS=false
    fi
    if [[ "$TARGET_INSTALLED" == false ]]; then
        log_err "Prerequisite check failed: Rust target '${TARGET}' is not installed."
        log_info "Remediation: Run 'rustup target add ${TARGET}'"
        CHECK_SUCCESS=false
    fi

    if [[ "$CHECK_SUCCESS" == false ]]; then
        log_err "Android JNI cross-compilation environment verification check FAILED."
        exit 1
    fi

    log_ok "Android JNI cross-compilation environment verification check completed successfully."
    exit 0
fi

# Resolve ABI name and toolchain prefix
ABI_NAME="$(get_abi_for_target "${TARGET}")"
CLANG_TRIPLE="$(get_clang_triple_for_target "${TARGET}")"
TARGET_ENV_VAR="$(echo "${TARGET}" | tr '-' '_' | tr '.' '_')"
TARGET_ENV_VAR_UPPER="$(echo "${TARGET_ENV_VAR}" | tr '[:lower:]' '[:upper:]')"

# Configure toolchain variables if NDK is present
STRIP_BIN=""
if [[ -n "$NDK_DIR" ]]; then
    TOOLCHAIN="${NDK_DIR}/toolchains/llvm/prebuilt/${HOST_TAG}"
    CLANG_BIN="${TOOLCHAIN}/bin/${CLANG_TRIPLE}${API_LEVEL}-clang"
    AR_BIN="${TOOLCHAIN}/bin/llvm-ar"
    STRIP_BIN="${TOOLCHAIN}/bin/llvm-strip"

    # Handle Windows .cmd/.exe suffixes
    if [[ ! -f "$CLANG_BIN" && -f "${CLANG_BIN}.cmd" ]]; then
        CLANG_BIN="${CLANG_BIN}.cmd"
    fi
    if [[ ! -f "$STRIP_BIN" && -f "${STRIP_BIN}.exe" ]]; then
        STRIP_BIN="${STRIP_BIN}.exe"
    fi

    if [[ -f "$CLANG_BIN" ]]; then
        export "CC_${TARGET_ENV_VAR}=${CLANG_BIN}"
        export "AR_${TARGET_ENV_VAR}=${AR_BIN}"
        export "CARGO_TARGET_${TARGET_ENV_VAR_UPPER}_LINKER=${CLANG_BIN}"
        log_ok "Configured Clang cross-linker: ${CLANG_BIN}"
    fi
fi

PROFILE_FLAG=""
if [[ "$BUILD_PROFILE" == "release" ]]; then
    PROFILE_FLAG="--release"
fi

BUILD_CMD="cargo build --target ${TARGET} -p rusty_grid_android_bridge ${PROFILE_FLAG} ${CARGO_FLAGS}"

if [[ "$DRY_RUN" == true ]]; then
    log_ok "[DRY-RUN] JNI Execution plan confirmed."
    echo "Command: ${BUILD_CMD}"
    echo "Destination: ${SCRIPT_DIR}/app/app/src/main/jniLibs/${ABI_NAME}/liboxideworker.so"
    exit 0
fi

# Execute build
if [[ -z "$NDK_DIR" ]]; then
    log_err "Android NDK is required for native shared library linking."
    log_err "Please set ANDROID_NDK_HOME or run with --dry-run."
    exit 1
fi

log_info "Executing JNI Build: ${BUILD_CMD}"
eval "${BUILD_CMD}"

# Locate compiled .so
OUTPUT_SO="${WORKSPACE_ROOT}/target/${TARGET}/${BUILD_PROFILE}/liboxideworker.so"
if [[ -f "$OUTPUT_SO" ]]; then
    # Strip debug symbols if strip binary is available and release build
    if [[ -n "$STRIP_BIN" && -x "$STRIP_BIN" && "$BUILD_PROFILE" == "release" ]]; then
        log_info "Stripping symbols from liboxideworker.so..."
        "${STRIP_BIN}" --strip-unneeded "${OUTPUT_SO}" || true
    fi

    SO_SIZE=$(ls -lh "$OUTPUT_SO" | awk '{print $5}')
    log_ok "Compiled liboxideworker.so (${SO_SIZE})"

    # Copy to Android Studio project jniLibs
    DEST_DIR="${SCRIPT_DIR}/app/app/src/main/jniLibs/${ABI_NAME}"
    mkdir -p "${DEST_DIR}"
    cp -f "${OUTPUT_SO}" "${DEST_DIR}/liboxideworker.so"
    log_ok "Deployed library to: ${DEST_DIR}/liboxideworker.so"

    echo "=================================================================="
    echo "Android Native JNI Shared Library successfully deployed!"
    echo "Location: ${DEST_DIR}/liboxideworker.so (${SO_SIZE})"
    echo "Engine:   In-Process POSIX pthreads / Multi-threaded Tokio"
    echo "PPK:      100% Immune (0 child processes)"
    echo "=================================================================="
else
    log_err "Expected shared library not found at ${OUTPUT_SO}"
    exit 1
fi
