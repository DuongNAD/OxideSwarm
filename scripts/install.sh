#!/bin/bash
# Install OxideSwarm binaries (oxideswarm, rusty-grid, ox-mode) from a pinned release.
# Design 0059 D6 (ADOPT-3): user-local install only; no sudo, no service, no daemon start,
# no secret handling, and no edits to ~/.oxideswarm.
# Compatible with bash 3.2 (macOS /bin/bash) and Linux bash.
set -eu

DEFAULT_VERSION="0.4.4"
REPO="DuongNAD/OxideSwarm"
SIGNER_WORKFLOW="DuongNAD/OxideSwarm/.github/workflows/release.yml"
DEFAULT_DOWNLOAD_BASE="https://github.com/DuongNAD/OxideSwarm/releases/download"

VERSION="${DEFAULT_VERSION}"
TARGET_DIR=""
DRY_RUN=0
GPU=0
TMP_DIR=""

usage() {
  cat <<'EOF'
Usage: install.sh [--version <VERSION>] [--dir <DIR>] [--gpu] [--dry-run] [-h|--help]

Install oxideswarm, rusty-grid, and ox-mode for the current user after verifying
SHA256SUMS (and GitHub build provenance attestation when `gh` is installed).

Options:
  --version <VERSION>  Release version to install (default: 0.4.4)
  --dir <DIR>          Destination directory (default: $HOME/.local/bin)
  --gpu                Install the -gpu build (with wgpu support)
  --dry-run            Print resolved target and URLs without downloading or writing files
  -h, --help           Show this help message and exit

Environment:
  OXIDESWARM_DOWNLOAD_BASE  Base URL for release downloads
                            (default: https://github.com/DuongNAD/OxideSwarm/releases/download)
EOF
}

cleanup() {
  if [ -n "${TMP_DIR}" ] && [ -d "${TMP_DIR}" ]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT INT TERM

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      if [ "$#" -lt 2 ]; then
        printf 'error: --version requires a value\n' >&2
        exit 2
      fi
      VERSION="$2"
      shift 2
      ;;
    --version=*)
      VERSION="${1#--version=}"
      shift
      ;;
    --dir)
      if [ "$#" -lt 2 ]; then
        printf 'error: --dir requires a value\n' >&2
        exit 2
      fi
      TARGET_DIR="$2"
      shift 2
      ;;
    --dir=*)
      TARGET_DIR="${1#--dir=}"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --gpu)
      GPU=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'error: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

VERSION="${VERSION#v}"
case "${VERSION}" in
  ""|*[!0-9A-Za-z._-]*|*..*|/*)
    printf 'error: invalid version: %s\n' "${VERSION}" >&2
    exit 2
    ;;
esac
if ! printf '%s\n' "${VERSION}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$'; then
  printf 'error: invalid version: %s\n' "${VERSION}" >&2
  exit 2
fi

if [ -z "${TARGET_DIR}" ]; then
  if [ -z "${HOME:-}" ]; then
    printf 'error: HOME is not set; pass --dir <DIR>\n' >&2
    exit 1
  fi
  TARGET_DIR="${HOME}/.local/bin"
fi

OS_NAME=$(uname -s)
ARCH_NAME=$(uname -m)
case "${OS_NAME}:${ARCH_NAME}" in
  Darwin:arm64|Darwin:aarch64)
    TARGET="aarch64-apple-darwin"
    ;;
  Darwin:x86_64)
    TARGET="x86_64-apple-darwin"
    ;;
  Linux:x86_64|Linux:amd64)
    TARGET="x86_64-unknown-linux-gnu"
    ;;
  *)
    printf 'error: unsupported platform (%s / %s): no release archive exists for OS "%s" and architecture "%s" (supported: Darwin arm64/x86_64, Linux x86_64)\n' \
      "${OS_NAME}" "${ARCH_NAME}" "${OS_NAME}" "${ARCH_NAME}" >&2
    exit 1
    ;;
esac

DOWNLOAD_BASE="${OXIDESWARM_DOWNLOAD_BASE:-${DEFAULT_DOWNLOAD_BASE}}"
while [ "${DOWNLOAD_BASE%/}" != "${DOWNLOAD_BASE}" ]; do
  DOWNLOAD_BASE="${DOWNLOAD_BASE%/}"
done
if [ -z "${DOWNLOAD_BASE}" ]; then
  printf 'error: OXIDESWARM_DOWNLOAD_BASE must not be empty\n' >&2
  exit 2
fi

if [ "${GPU}" = "1" ]; then
  ARCHIVE_STEM="oxideswarm-${VERSION}-${TARGET}-gpu"
else
  ARCHIVE_STEM="oxideswarm-${VERSION}-${TARGET}"
fi
ARCHIVE="${ARCHIVE_STEM}.tar.gz"
ARCHIVE_URL="${DOWNLOAD_BASE}/v${VERSION}/${ARCHIVE}"
SUMS_URL="${DOWNLOAD_BASE}/v${VERSION}/SHA256SUMS"

if command -v gh >/dev/null 2>&1; then
  ATTEST_PLAN="verify with gh attestation verify"
else
  ATTEST_PLAN="skip (gh CLI not found; SHA256SUMS only)"
fi

if [ "${DRY_RUN}" = "1" ]; then
  printf 'Dry run: no files will be downloaded or written.\n'
  printf '  version:     %s\n' "${VERSION}"
  printf '  platform:    %s / %s -> %s\n' "${OS_NAME}" "${ARCH_NAME}" "${TARGET}"
  printf '  archive:     %s\n' "${ARCHIVE_URL}"
  printf '  checksums:   %s\n' "${SUMS_URL}"
  printf '  attestation: %s\n' "${ATTEST_PLAN}"
  printf '  destination: %s\n' "${TARGET_DIR}"
  exit 0
fi

download_file() {
  _url="$1"
  _dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "${_url}" -o "${_dest}"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "${_dest}" "${_url}"
  else
    printf 'error: neither curl nor wget is installed\n' >&2
    return 1
  fi
}

sha256_file() {
  _file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${_file}" | awk '{print tolower($1)}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${_file}" | awk '{print tolower($1)}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "${_file}" | awk '{print tolower($NF)}'
  else
    printf 'error: no SHA-256 tool found (need sha256sum, shasum, or openssl)\n' >&2
    return 1
  fi
}

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/oxideswarm-install.XXXXXX")
ARCHIVE_PATH="${TMP_DIR}/${ARCHIVE}"
SUMS_PATH="${TMP_DIR}/SHA256SUMS"
UNPACK_DIR="${TMP_DIR}/unpack"

printf 'Downloading %s...\n' "${ARCHIVE_URL}"
download_file "${ARCHIVE_URL}" "${ARCHIVE_PATH}"
printf 'Downloading %s...\n' "${SUMS_URL}"
download_file "${SUMS_URL}" "${SUMS_PATH}"

EXPECTED_SHA=$(awk -v fname="${ARCHIVE}" '
  {
    line = $0
    sub(/\r$/, "", line)
    idx = index(line, " ")
    if (idx == 0) idx = index(line, "\t")
    if (idx == 65) {
      h = substr(line, 1, 64)
      rest = substr(line, 66)
      while (substr(rest, 1, 1) == " " || substr(rest, 1, 1) == "\t") {
        rest = substr(rest, 2)
      }
      if (substr(rest, 1, 1) == "*") rest = substr(rest, 2)
      if (substr(rest, 1, 2) == "./") rest = substr(rest, 3)
      if (rest == fname) {
        print tolower(h)
        exit
      }
    }
  }
' "${SUMS_PATH}" | grep -E '^[0-9a-f]{64}$' || true)

if [ -z "${EXPECTED_SHA}" ]; then
  printf 'error: %s entry not found in SHA256SUMS\n' "${ARCHIVE}" >&2
  exit 1
fi

ACTUAL_SHA=$(sha256_file "${ARCHIVE_PATH}" | grep -E '^[0-9a-f]{64}$' || true)
if [ -z "${ACTUAL_SHA}" ]; then
  printf 'error: failed to compute SHA-256 checksum for %s\n' "${ARCHIVE}" >&2
  exit 1
fi

if [ "${ACTUAL_SHA}" != "${EXPECTED_SHA}" ]; then
  printf 'error: SHA-256 checksum mismatch for %s\n' "${ARCHIVE}" >&2
  printf '  expected: %s\n' "${EXPECTED_SHA}" >&2
  printf '  actual:   %s\n' "${ACTUAL_SHA}" >&2
  exit 1
fi
printf 'Checksum verified (%s).\n' "${ACTUAL_SHA}"

if command -v gh >/dev/null 2>&1; then
  printf 'Verifying release provenance attestation with gh...\n'
  if ! gh attestation verify "${ARCHIVE_PATH}" \
    --repo "${REPO}" \
    --signer-workflow "${SIGNER_WORKFLOW}" \
    --source-ref "refs/tags/v${VERSION}"; then
    printf 'error: provenance attestation verification failed for %s\n' "${ARCHIVE}" >&2
    exit 1
  fi
  printf 'Provenance attestation verified.\n'
else
  printf 'Note: gh CLI not found; skipping provenance attestation check (SHA-256 verified).\n'
fi

mkdir -p "${UNPACK_DIR}"
tar -xzf "${ARCHIVE_PATH}" -C "${UNPACK_DIR}"

if [ -L "${UNPACK_DIR}/${ARCHIVE_STEM}" ]; then
  printf 'error: archive entry "%s" is a symlink\n' "${ARCHIVE_STEM}" >&2
  exit 1
elif [ -d "${UNPACK_DIR}/${ARCHIVE_STEM}" ]; then
  SRC_DIR="${UNPACK_DIR}/${ARCHIVE_STEM}"
else
  SRC_DIR="${UNPACK_DIR}"
fi

for bin in oxideswarm rusty-grid ox-mode; do
  if [ ! -f "${SRC_DIR}/${bin}" ] || [ -L "${SRC_DIR}/${bin}" ]; then
    printf 'error: expected binary "%s" missing or unsafe in archive %s\n' "${bin}" "${ARCHIVE}" >&2
    exit 1
  fi
done

if [ -L "${TARGET_DIR}" ] || { [ -e "${TARGET_DIR}" ] && [ ! -d "${TARGET_DIR}" ]; }; then
  printf 'error: destination %s exists and is not a regular directory\n' "${TARGET_DIR}" >&2
  exit 1
fi

mkdir -p "${TARGET_DIR}"
for bin in oxideswarm rusty-grid ox-mode; do
  _tmp_bin="${TARGET_DIR}/.${bin}.tmp.$$"
  cp "${SRC_DIR}/${bin}" "${_tmp_bin}"
  chmod 0755 "${_tmp_bin}"
  mv -f "${_tmp_bin}" "${TARGET_DIR}/${bin}"
done

printf '\nInstalled OxideSwarm %s (%s) into %s\n' "${VERSION}" "${TARGET}" "${TARGET_DIR}"
case ":${PATH:-}:" in
  *":${TARGET_DIR}:"*) ;;
  *)
    printf 'Add it to PATH for this shell:\n'
    printf '  export PATH="%s:$PATH"\n' "${TARGET_DIR}"
    ;;
esac
printf '\nQuick start (three terminals, loopback):\n'
printf '  oxideswarm master\n'
printf '  oxideswarm worker --workspaces\n'
printf '  oxideswarm submit --wait -- hostname\n'
