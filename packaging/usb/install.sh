#!/bin/bash
# OxideSwarm USB installer for macOS and Linux (kit format v0, design 0053 prototype).
#
# Installs the worker for the current user from this USB stick, joins the cluster with the kit's
# join token, starts the worker, and starts it again at every login. Nothing outside the install
# folder is changed except the per-user autostart entry. The user's own ~/.oxideswarm is never
# read or written: the installed worker keeps its state in <install folder>/home.
#
#   bash install.sh [--name NAME] [--install-root DIR] [--no-autostart] [--workspaces|--no-workspaces] [--isolation off|fs]
#                   [--gpu|--no-gpu] [--slots N] [--update]
#
# --slots N sets how many tasks the worker runs at once. Without it the installer uses the logical
# processors of the performance cores, at most half of all processors (USB-4).
#
# --update (what "Update OxideSwarm.command" runs) updates an existing install in place: it keeps
# its folder, name, GPU choice, isolation, cluster membership and start at login, asks nothing,
# does not join again, and only replaces the binaries and restarts the worker.
#
# Compatible with bash 3.2 (macOS /bin/bash).
set -eu

KIT_DIR=$(cd "$(dirname "$0")" && pwd)
NAME=""
ROOT=""
AUTOSTART=1
WORKSPACES=1
WORKSPACES_EXPLICIT=0
GPU=""
UPDATE=0
ISOLATION=""
EXPLICIT_ISOLATION=0
ISO_SET_BY_FLAG=0
SLOTS_SET=""

INSTALL_LOG_ACTIVE=""
INSTALL_LOG_TMP=$(mktemp "${TMPDIR:-/tmp}/oxideswarm-usb-install.XXXXXX" 2>/dev/null || true)
TMP=""
FRESH_INSTALL=0
PREEXISTING_ROOT=0
ROLLBACK_ARMED=0
CREATED_AUTOSTART_MAC=0
CREATED_AUTOSTART_SYS=0
CREATED_AUTOSTART_XDG=""
IS_V1_KIT=0
KIT_VERSION=""
KIT_WORKSPACES=""
KIT_HAS_WORKER=1
KIT_HAS_CLIENT=1

log_append() {
  if [ -n "$INSTALL_LOG_ACTIVE" ]; then
    printf '%s\n' "$*" >> "$INSTALL_LOG_ACTIVE" 2>/dev/null || true
  elif [ -n "$INSTALL_LOG_TMP" ]; then
    printf '%s\n' "$*" >> "$INSTALL_LOG_TMP" 2>/dev/null || true
  fi
}

say() {
  printf '%s\n' "$*"
  log_append "$*"
}

rollback_fresh_install() {
  [ "$ROLLBACK_ARMED" = 1 ] || return 0
  ROLLBACK_ARMED=0
  [ "$FRESH_INSTALL" = 1 ] || return 0
  if [ -n "${RUN_DIR:-}" ] && [ -f "$RUN_DIR/worker.pid" ]; then
    _rb_pid=$(cat "$RUN_DIR/worker.pid" 2>/dev/null || true)
    if [ -n "$_rb_pid" ]; then
      case "$(ps -ww -p "$_rb_pid" -o command= 2>/dev/null || true)" in
        *"${ROOT:-}/bin/oxideswarm "*) kill "$_rb_pid" 2>/dev/null || true ;;
      esac
    fi
  fi
  if [ "$CREATED_AUTOSTART_MAC" = 1 ]; then
    launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
    rm -f "$HOME/Library/LaunchAgents/com.oxideswarm.usb-worker.plist"
  fi
  if [ "$CREATED_AUTOSTART_SYS" = 1 ] && command -v systemctl >/dev/null 2>&1; then
    systemctl --user disable --now oxideswarm-usb-worker.service 2>/dev/null || true
    rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/oxideswarm-usb-worker.service"
    systemctl --user daemon-reload 2>/dev/null || true
  fi
  if [ -n "$CREATED_AUTOSTART_XDG" ]; then
    rm -f "$CREATED_AUTOSTART_XDG"
  fi
  if [ -n "${ROOT:-}" ] && [ -d "$ROOT" ]; then
    cd "${TMPDIR:-/tmp}" 2>/dev/null || cd / || true
    if [ "$PREEXISTING_ROOT" = 0 ]; then
      rm -rf "$ROOT"
    else
      rm -f "$ROOT/kit-install.conf" "$ROOT/start-worker.sh"
      rm -rf "${ROOT:?}/bin" "${ROOT:?}/home" "${ROOT:?}/run" "${ROOT:?}/logs" "${ROOT:?}/workspaces"
    fi
  fi
}

cleanup_on_exit() {
  if [ -n "$TMP" ] && [ -d "$TMP" ]; then rm -rf "$TMP"; fi
  if [ "$ROLLBACK_ARMED" = 1 ]; then rollback_fresh_install; fi
  if [ -n "$INSTALL_LOG_TMP" ] && [ -f "$INSTALL_LOG_TMP" ]; then rm -f "$INSTALL_LOG_TMP"; fi
}
trap cleanup_on_exit EXIT

fail() {
  log_append "ERROR: $*"
  printf 'ERROR: %s\n' "$*" >&2
  rollback_fresh_install
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --name) [ $# -ge 2 ] || fail "--name needs a value"; NAME=$2; shift ;;
    --install-root) [ $# -ge 2 ] || fail "--install-root needs a value"; ROOT=$2; shift ;;
    --no-autostart) AUTOSTART=0 ;;
    --no-workspaces) WORKSPACES=0; WORKSPACES_EXPLICIT=1 ;;
    --workspaces) WORKSPACES=1; WORKSPACES_EXPLICIT=1 ;;
    --update) UPDATE=1 ;;
    --gpu) [ "$GPU" = 0 ] && fail "--gpu and --no-gpu cannot be used together"; GPU=1 ;;
    --no-gpu) [ "$GPU" = 1 ] && fail "--gpu and --no-gpu cannot be used together"; GPU=0 ;;
    --isolation)
      [ $# -ge 2 ] || fail "--isolation needs a value (off or fs)"
      case "$2" in
        off|fs) ISOLATION=$2; EXPLICIT_ISOLATION=1; ISO_SET_BY_FLAG=1 ;;
        *) fail "--isolation must be off or fs (got: $2)" ;;
      esac
      shift ;;
    --slots)
      [ $# -ge 2 ] || fail "--slots needs a number from 1 to 256"
      case "$2" in
        ''|*[!0-9]*) fail "--slots needs a number from 1 to 256 (got: $2)" ;;
      esac
      { [ "$2" -ge 1 ] && [ "$2" -le 256 ]; } || fail "--slots needs a number from 1 to 256 (got: $2)"
      SLOTS_SET=$2
      shift ;;
    -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
    *) fail "unknown option: $1" ;;
  esac
  shift
done

OS=$(uname -s)
ARCH=$(uname -m)
case "$OS/$ARCH" in
  Darwin/arm64) TARGET=aarch64-apple-darwin ;;
  Darwin/x86_64) TARGET=x86_64-apple-darwin ;;
  Linux/x86_64) TARGET=x86_64-unknown-linux-gnu ;;
  *) fail "this kit has no build for $OS/$ARCH" ;;
esac

if [ -z "$ROOT" ]; then
  if [ "$OS" = Darwin ]; then
    ROOT="$HOME/Library/Application Support/OxideSwarm"
  else
    ROOT="${XDG_DATA_HOME:-$HOME/.local/share}/oxideswarm"
  fi
fi

INVITE_FILE="$KIT_DIR/invite.oxs"
KIT_JSON="$KIT_DIR/kit.json"
if [ -f "$INVITE_FILE" ]; then
  IS_V1_KIT=1
  [ -f "$KIT_JSON" ] || fail "kit.json is missing from this v1 kit"
fi
if [ -f "$KIT_JSON" ]; then
  _flat_json=$(tr '\r\n' '  ' < "$KIT_JSON")
  _kit_format=$(printf '%s\n' "$_flat_json" | awk '{ if (match($0, /"kit_format"[[:space:]]*:[[:space:]]*[0-9]+/)) { s=substr($0,RSTART,RLENGTH); sub(/^.*:[[:space:]]*/, "", s); print s } }')
  [ "$_kit_format" = "1" ] || fail "unsupported kit_format '$_kit_format' in kit.json (expected 1)"
  KIT_VERSION=$(printf '%s\n' "$_flat_json" | awk '{ if (match($0, /"version"[[:space:]]*:[[:space:]]*"[^"]*"/)) { s=substr($0,RSTART,RLENGTH); sub(/^.*:[[:space:]]*"/, "", s); sub(/"$/, "", s); print s } }')
  case "$KIT_VERSION" in
    ''|*..*|*/*|*\\*|*[!A-Za-z0-9.+_-]*) fail "invalid version '$KIT_VERSION' in kit.json" ;;
  esac
  _ws_raw=$(printf '%s\n' "$_flat_json" | awk '{ if (match($0, /"workspaces"[[:space:]]*:[[:space:]]*(true|false)/)) { s=substr($0,RSTART,RLENGTH); sub(/^.*:[[:space:]]*/, "", s); print s } }')
  case "$_ws_raw" in
    true) KIT_WORKSPACES=1 ;;
    false) KIT_WORKSPACES=0 ;;
    *) KIT_WORKSPACES="" ;;
  esac
  _roles_raw=$(printf '%s\n' "$_flat_json" | awk '{ if (match($0, /"roles"[[:space:]]*:[[:space:]]*\[[^]]*\]/)) { print substr($0,RSTART,RLENGTH) } }')
  [ -n "$_roles_raw" ] || fail "kit.json is missing roles"
  if printf '%s\n' "$_roles_raw" | grep -q '"worker"'; then KIT_HAS_WORKER=1; else KIT_HAS_WORKER=0; fi
  if printf '%s\n' "$_roles_raw" | grep -q '"client"'; then KIT_HAS_CLIENT=1; else KIT_HAS_CLIENT=0; fi
  [ "$KIT_HAS_WORKER" = 1 ] || [ "$KIT_HAS_CLIENT" = 1 ] || fail "kit.json roles lists neither worker nor client"
fi
[ "$UPDATE" = 1 ] || [ "$IS_V1_KIT" = 1 ] || [ -f "$KIT_DIR/join.token" ] || fail "join.token (or invite.oxs) is missing from the kit"
# A kit made with make_kit.sh --no-autostart is meant for a trial on someone else's machine.
[ -f "$KIT_DIR/no-autostart" ] && AUTOSTART=0
if [ "$IS_V1_KIT" = 1 ] && [ "$UPDATE" = 0 ] && [ "$WORKSPACES_EXPLICIT" = 0 ] && [ -n "$KIT_WORKSPACES" ]; then
  WORKSPACES=$KIT_WORKSPACES
fi
if [ "$KIT_HAS_WORKER" = 0 ]; then
  GPU=0
fi
if [ -z "$ISOLATION" ]; then
  if [ -f "$KIT_DIR/isolation" ]; then
    BAKED_ISO=$(tr -d ' \r\n\t' < "$KIT_DIR/isolation")
    case "$BAKED_ISO" in
      off|fs) ISOLATION=$BAKED_ISO; EXPLICIT_ISOLATION=1 ;;
      *) fail "the kit's isolation file holds '$BAKED_ISO' (expected 'off' or 'fs')" ;;
    esac
  else
    ISOLATION=fs
  fi
fi
[ -f "$KIT_DIR/archives/SHA256SUMS" ] || fail "archives/SHA256SUMS is missing from the kit"

PLIST="$HOME/Library/LaunchAgents/com.oxideswarm.usb-worker.plist"
UNIT="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/oxideswarm-usb-worker.service"
XDG_DESKTOP="${XDG_CONFIG_HOME:-$HOME/.config}/autostart/oxideswarm-usb-worker.desktop"
# Whether this install's worker is started at login now (a LaunchAgent, systemd user unit, or
# XDG autostart entry that runs this install's binary/launcher).
has_autostart() {
  { [ -f "$PLIST" ] && grep -qF "$ROOT/bin/oxideswarm" "$PLIST"; } ||
    { [ -f "$UNIT" ] && grep -qF "$ROOT/bin/oxideswarm" "$UNIT"; } ||
    { [ -f "$XDG_DESKTOP" ] && grep -qF "$ROOT/start-worker.sh" "$XDG_DESKTOP"; }
}
if [ "$UPDATE" = 1 ]; then
  [ -x "$ROOT/bin/oxideswarm" ] || fail "there is no OxideSwarm from a USB kit in $ROOT; run the installer first"
  # Its settings: kit-install.conf (written since the update mode exists), else what is on disk.
  if [ -f "$ROOT/kit-install.conf" ]; then
    while IFS='=' read -r key value; do
      case "$key" in
        name) [ -n "$NAME" ] || NAME=$value ;;
        gpu) [ -n "$GPU" ] || case "$value" in 0|1) GPU=$value ;; esac ;;
        workspaces) [ "$WORKSPACES_EXPLICIT" = 1 ] || case "$value" in 0|1) WORKSPACES=$value ;; esac ;;
        worker) [ "$IS_V1_KIT" = 1 ] || case "$value" in 0|1) KIT_HAS_WORKER=$value ;; esac ;;
        client) [ "$IS_V1_KIT" = 1 ] || case "$value" in 0|1) KIT_HAS_CLIENT=$value ;; esac ;;
        # The isolation the worker ran with is kept, and an update never weakens it silently.
        isolation) [ "$ISO_SET_BY_FLAG" = 1 ] || case "$value" in off|fs) ISOLATION=$value; EXPLICIT_ISOLATION=1 ;; esac ;;
        # Slots chosen with --slots at install time are kept; an automatic choice is made again.
        slots) [ -n "$SLOTS_SET" ] || case "$value" in ''|*[!0-9]*) ;; *) [ "$value" -ge 1 ] && [ "$value" -le 256 ] && SLOTS_SET=$value ;; esac ;;
      esac
    done < "$ROOT/kit-install.conf"
  fi
  if [ -z "$NAME" ]; then
    ID_FILE=$(ls -t "$ROOT"/home/worker_id.* 2>/dev/null | head -n 1 || true)
    [ -n "$ID_FILE" ] && NAME=${ID_FILE##*/worker_id.}
  fi
  case "$NAME" in *[!A-Za-z0-9_.-]*) NAME="" ;; esac
  # The start at login stays as it is.
  if has_autostart; then AUTOSTART=1; else AUTOSTART=0; fi
fi

ARCHIVE=""
if [ "$IS_V1_KIT" = 1 ] && [ -n "$KIT_VERSION" ] && [ -f "$KIT_DIR/archives/oxideswarm-$KIT_VERSION-$TARGET.tar.gz" ]; then
  ARCHIVE="$KIT_DIR/archives/oxideswarm-$KIT_VERSION-$TARGET.tar.gz"
else
  for f in "$KIT_DIR"/archives/oxideswarm-*-"$TARGET".tar.gz; do
    [ -f "$f" ] && ARCHIVE=$f
  done
fi
[ -f "$ARCHIVE" ] || fail "no archive for $TARGET in $KIT_DIR/archives"
# A kit made with make_kit.sh --gpu also holds the gpu-wgpu build, which uses this machine's GPU
# when it finds one. It is installed only when the person at the keyboard agrees (or --gpu).
GPU_ARCHIVE=""
if [ "$IS_V1_KIT" = 1 ] && [ -n "$KIT_VERSION" ] && [ -f "$KIT_DIR/archives/oxideswarm-$KIT_VERSION-$TARGET-gpu.tar.gz" ]; then
  GPU_ARCHIVE="$KIT_DIR/archives/oxideswarm-$KIT_VERSION-$TARGET-gpu.tar.gz"
else
  for f in "$KIT_DIR"/archives/oxideswarm-*-"$TARGET"-gpu.tar.gz; do
    [ -f "$f" ] && GPU_ARCHIVE=$f
  done
fi
if [ "$GPU" = 1 ] && [ -z "$GPU_ARCHIVE" ]; then
  [ "$UPDATE" = 1 ] && fail "this install uses the GPU build, which this kit lacks; update from a kit made with make_kit.sh --gpu, or add --no-gpu"
  fail "this kit has no GPU build for $TARGET (make it with make_kit.sh --gpu)"
fi

if [ "$UPDATE" = 1 ]; then say "OxideSwarm USB installer (update)"; else say "OxideSwarm USB installer"; fi
say "  system:  $OS $ARCH ($TARGET)"
say "  folder:  $ROOT"

if [ -n "$GPU_ARCHIVE" ] && [ -z "$GPU" ]; then
  GPU=0
  if [ -t 0 ]; then
    printf "Let the cluster use this computer's GPU as well? [Y/n] "
    read -r answer || answer=n
    case "$answer" in [Nn]*) ;; *) GPU=1 ;; esac
  fi
fi
if [ -n "$GPU_ARCHIVE" ] && [ "$GPU" = 1 ]; then
  ARCHIVE=$GPU_ARCHIVE
  say "  gpu:     yes (GPU build; the worker offers the GPU it finds)"
elif [ -n "$GPU_ARCHIVE" ]; then
  say "  gpu:     no (CPU build; run the installer with --gpu to change that)"
fi
ARCHIVE_NAME=$(basename "$ARCHIVE")

# 1. Check the archive against the kit's SHA256SUMS (and online release SHA256SUMS when
#    configured or reachable).
lookup_sum_entry() {
  _sums_file=$1
  _target_name=$2
  awk -v n="$_target_name" '
    /^[[:space:]]*(#|$)/ { next }
    {
      h = $1
      f = $2
      sub(/^\*/, "", f)
      if (f == n && h ~ /^[0-9a-fA-F]{64}$/) {
        print tolower(h)
        exit
      }
    }' "$_sums_file"
}

verify_online_sums() {
  _v_name=$1
  _v_expected=$2
  _online_url=${OXIDESWARM_USB_ONLINE_SUMS_URL:-}
  _strict=0
  case "$_online_url" in
    off|OFF|0|none|NONE)
      say "  online:  skipped (OXIDESWARM_USB_ONLINE_SUMS_URL=off)"
      return 0 ;;
    '')
      [ "$IS_V1_KIT" = 1 ] && [ -n "$KIT_VERSION" ] || return 0
      _online_url="https://github.com/DuongNAD/OxideSwarm/releases/download/v${KIT_VERSION}/SHA256SUMS"
      _strict=0 ;;
    *)
      _strict=1 ;;
  esac

  _online_tmp=$(mktemp "${TMPDIR:-/tmp}/oxideswarm-online-sums.XXXXXX")
  _fetched=0
  case "$_online_url" in
    file://localhost/*)
      _local_sums=${_online_url#file://localhost}
      if [ -f "$_local_sums" ] && [ ! -L "$_local_sums" ]; then
        cp "$_local_sums" "$_online_tmp" 2>/dev/null && _fetched=1
      fi
      [ "$_fetched" = 1 ] || { rm -f "$_online_tmp"; fail "cannot read online SHA256SUMS from $_online_url"; } ;;
    file:///*)
      _local_sums=${_online_url#file://}
      if [ -f "$_local_sums" ] && [ ! -L "$_local_sums" ]; then
        cp "$_local_sums" "$_online_tmp" 2>/dev/null && _fetched=1
      fi
      [ "$_fetched" = 1 ] || { rm -f "$_online_tmp"; fail "cannot read online SHA256SUMS from $_online_url"; } ;;
    https://*)
      case "$_online_url" in
        https://*:*@*|https://*@*)
          rm -f "$_online_tmp"
          fail "refusing online SHA256SUMS URL with embedded credentials" ;;
      esac
      if command -v curl >/dev/null 2>&1; then
        if curl -fsSL --proto =https --connect-timeout 5 --max-time 12 -o "$_online_tmp" "$_online_url" 2>/dev/null; then
          _fetched=1
        fi
      fi
      if [ "$_fetched" != 1 ]; then
        rm -f "$_online_tmp"
        if [ "$_strict" = 1 ]; then
          fail "could not fetch online SHA256SUMS from $_online_url"
        else
          say "  online:  unavailable (offline or unreleased v$KIT_VERSION); relying on stick SHA256SUMS"
          return 0
        fi
      fi ;;
    *)
      rm -f "$_online_tmp"
      fail "refusing online SHA256SUMS URL '$_online_url': only https:// or file:// is allowed" ;;
  esac

  _online_expected=$(lookup_sum_entry "$_online_tmp" "$_v_name")
  rm -f "$_online_tmp"
  [ -n "$_online_expected" ] || fail "$_v_name is not listed in online SHA256SUMS"
  [ "$_online_expected" = "$_v_expected" ] || fail "online SHA256SUMS mismatch for $_v_name (stick $_v_expected vs online $_online_expected); refusing tampered kit"
  say "  online:  $_v_name matches online SHA256SUMS"
}

EXPECTED=$(lookup_sum_entry "$KIT_DIR/archives/SHA256SUMS" "$ARCHIVE_NAME")
[ -n "$EXPECTED" ] || fail "$ARCHIVE_NAME is not listed in SHA256SUMS"
if command -v shasum >/dev/null 2>&1; then
  ACTUAL=$(shasum -a 256 "$ARCHIVE" | awk '{print tolower($1)}')
else
  ACTUAL=$(sha256sum "$ARCHIVE" | awk '{print tolower($1)}')
fi
[ "$EXPECTED" = "$ACTUAL" ] || fail "checksum mismatch for $ARCHIVE_NAME: the copy on the stick is damaged"
say "  archive: $ARCHIVE_NAME (checksum OK)"
verify_online_sums "$ARCHIVE_NAME" "$EXPECTED"

# Stop the worker this install's start-at-login entry keeps alive before its binary changes; step 8
# puts the entry back.
if has_autostart; then
  if [ "$OS" = Darwin ]; then
    launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
  else
    if command -v systemctl >/dev/null 2>&1; then
      systemctl --user stop oxideswarm-usb-worker.service 2>/dev/null || true
    fi
    if [ -f "$ROOT/run/worker.pid" ]; then
      _old_xdg_pid=$(cat "$ROOT/run/worker.pid" 2>/dev/null || true)
      if [ -n "$_old_xdg_pid" ]; then
        case "$(ps -ww -p "$_old_xdg_pid" -o command= 2>/dev/null || true)" in
          *"$ROOT/bin/oxideswarm "*) kill "$_old_xdg_pid" 2>/dev/null || true ;;
        esac
      fi
    fi
  fi
fi

# 2. Unpack the binaries.
BIN_DIR="$ROOT/bin"
STATE="$ROOT/home"
RUN_DIR="$ROOT/run"
LOG_DIR="$ROOT/logs"
if [ -d "$ROOT" ]; then PREEXISTING_ROOT=1; fi
if [ "$UPDATE" != 1 ] && [ ! -e "$BIN_DIR/oxideswarm" ] && [ ! -e "$STATE/node.key" ] && [ ! -e "$STATE/cluster.secret" ]; then
  FRESH_INSTALL=1
  ROLLBACK_ARMED=1
fi
mkdir -p "$BIN_DIR" "$STATE" "$RUN_DIR" "$LOG_DIR"
chmod 700 "$STATE"
INSTALL_LOG_ACTIVE="$LOG_DIR/install.log"
{
  printf '=== OxideSwarm USB installer run (%s) ===\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date)"
  if [ -n "$INSTALL_LOG_TMP" ] && [ -f "$INSTALL_LOG_TMP" ]; then
    cat "$INSTALL_LOG_TMP"
    rm -f "$INSTALL_LOG_TMP"
    INSTALL_LOG_TMP=""
  fi
} >> "$INSTALL_LOG_ACTIVE" 2>/dev/null || true
chmod 600 "$INSTALL_LOG_ACTIVE" 2>/dev/null || true
TMP=$(mktemp -d "${TMPDIR:-/tmp}/oxideswarm-usb.XXXXXX")
tar -xzf "$ARCHIVE" -C "$TMP"
SRC=$(find "$TMP" -type f -name oxideswarm | head -n 1)
[ -n "$SRC" ] || fail "the archive holds no oxideswarm binary"
SRC_DIR=$(dirname "$SRC")
for b in oxideswarm rusty-grid ox-mode; do
  if [ -f "$SRC_DIR/$b" ]; then
    cp "$SRC_DIR/$b" "$BIN_DIR/$b.new"
    chmod 755 "$BIN_DIR/$b.new"
    mv -f "$BIN_DIR/$b.new" "$BIN_DIR/$b"
  fi
done
BIN="$BIN_DIR/oxideswarm"
"$BIN" --version >/dev/null 2>&1 || fail "the unpacked binary does not run on this machine"
say "  binary:  $("$BIN" --version)"

# Every command below keeps its state in $STATE and runs from $ROOT, so neither ~/.oxideswarm nor
# a rusty-grid.toml in the current folder is used.
export OXIDESWARM_HOME="$STATE"
cd "$ROOT"

# 3. Stop a worker that an earlier run of this installer started.
if [ -f "$RUN_DIR/worker.pid" ]; then
  OLD=$(cat "$RUN_DIR/worker.pid" 2>/dev/null || true)
  # A PID file can outlive a reboot, and its number then names another process: only one that runs
  # this install's binary is stopped.
  case "$(ps -ww -p "$OLD" -o command= 2>/dev/null)" in
    *"$BIN_DIR/oxideswarm "*) ;;
    *) OLD="" ;;
  esac
  if [ -n "$OLD" ] && kill -0 "$OLD" 2>/dev/null; then
    say "  stopping the worker started earlier (PID $OLD)"
    kill "$OLD" 2>/dev/null || true
    sleep 2
  fi
  rm -f "$RUN_DIR/worker.pid"
fi

# 4. Join: the token (v0) is read from stdin; the encrypted invite (v1) reads the passphrase
#    and confirmation from stdin directly. An update keeps the existing membership.
if [ "$UPDATE" = 0 ]; then
  say ""
  say "Joining the cluster ..."
  if [ "$IS_V1_KIT" = 1 ]; then
    if ! "$BIN" join --invite-file "$INVITE_FILE" --no-doctor; then
      fail "join failed (see the message above); nothing was started"
    fi
  else
    if ! "$BIN" join --no-doctor < "$KIT_DIR/join.token"; then
      fail "join failed (see the message above); nothing was started"
    fi
  fi
fi

# The worker prefers a P2P ticket that join saved in [worker] p2p_ticket. On networks where the
# machines cannot reach each other directly (school or office Wi-Fi with client isolation), the
# client checks below must use that ticket too. A ticket is an address, not a credential.
TICKET=$(awk '
  /^\[/ { in_worker = ($0 ~ /^\[worker\][[:space:]]*$/) }
  in_worker && /^[[:space:]]*p2p_ticket[[:space:]]*=/ {
    v = $0; sub(/^[^=]*=[[:space:]]*/, "", v); sub(/[[:space:]]+$/, "", v)
    if (v ~ /^'"'"'.*'"'"'$/ || v ~ /^".*"$/) v = substr(v, 2, length(v) - 2)
    print v; exit
  }' "$STATE/rusty-grid.toml" 2>/dev/null || true)
if [ -n "$TICKET" ]; then
  say "  link:    P2P ticket (the worker and the checks connect through iroh)"
fi

# 5. Worker options.
if [ -z "$NAME" ]; then
  NAME=$(hostname -s 2>/dev/null || hostname)
  NAME=$(printf '%s' "$NAME" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_-' '-' | sed 's/^-*//; s/-*$//')
  [ -n "$NAME" ] || NAME=worker
fi
if [ "$KIT_HAS_WORKER" = 1 ]; then
# A stable worker id for this machine and name in this cluster, so that a reinstall shows up as the
# same worker instead of a new one beside a disconnected old entry. It is a SHA-256 over the
# machine's own id, the name and the cluster secret (or master_key on v1 invite kits): the machine id
# never leaves the machine, and the same machine gets unrelated ids in different clusters. An existing
# id file is kept. OXIDESWARM_USB_MACHINE_ID replaces the machine id (tests).
ID_FILE="$STATE/worker_id.$NAME"
if [ ! -s "$ID_FILE" ]; then
  MACHINE_ID=${OXIDESWARM_USB_MACHINE_ID:-}
  if [ -z "$MACHINE_ID" ] && [ "$OS" = Darwin ]; then
    MACHINE_ID=$(ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null | awk -F'"' '/IOPlatformUUID/ { print $4; exit }' || true)
  fi
  if [ -z "$MACHINE_ID" ]; then
    for f in /etc/machine-id /var/lib/dbus/machine-id; do
      if [ -s "$f" ]; then MACHINE_ID=$(cat "$f"); break; fi
    done
  fi
  if [ -n "$MACHINE_ID" ]; then
    if command -v shasum >/dev/null 2>&1; then SHA="shasum -a 256"; else SHA=sha256sum; fi
    if [ -s "$STATE/cluster.secret" ]; then
      H=$({ printf 'oxideswarm-usb-worker-id-v1\n%s\n%s\n' "$MACHINE_ID" "$NAME"; cat "$STATE/cluster.secret" 2>/dev/null || true; } | $SHA | cut -c1-32)
    else
      MASTER_KEY_SEED=""
      if [ -s "$STATE/master_key" ]; then
        MASTER_KEY_SEED=$(cat "$STATE/master_key" 2>/dev/null || true)
      elif [ -f "$STATE/rusty-grid.toml" ]; then
        MASTER_KEY_SEED=$(awk '
          /^[[:space:]]*master_key[[:space:]]*=/ {
            v = $0; sub(/^[^=]*=[[:space:]]*/, "", v); sub(/[[:space:]]*(#.*)?$/, "", v)
            if (v ~ /^'"'"'.*'"'"'$/ || v ~ /^".*"$/) v = substr(v, 2, length(v) - 2)
            print v; exit
          }' "$STATE/rusty-grid.toml" 2>/dev/null || true)
      fi
      H=$({ printf 'oxideswarm-usb-worker-id-v1\n%s\n%s\n' "$MACHINE_ID" "$NAME"; if [ -n "$MASTER_KEY_SEED" ]; then printf '%s\n' "$MASTER_KEY_SEED"; fi; } | $SHA | cut -c1-32)
    fi
    # UUID version 8 (vendor-specific) with the RFC 9562 variant bits.
    VARIANT=$(printf '%x' $(( (0x$(printf '%s' "$H" | cut -c17) & 3) | 8 )))
    UUID="$(printf '%s' "$H" | cut -c1-8)-$(printf '%s' "$H" | cut -c9-12)-8$(printf '%s' "$H" | cut -c14-16)-$VARIANT$(printf '%s' "$H" | cut -c18-20)-$(printf '%s' "$H" | cut -c21-32)"
    (umask 077; printf '%s\n' "$UUID" > "$ID_FILE")
  fi
fi
# Task slots (USB-4): the logical processors of the performance cores, at most half of all
# processors and at least 1, so the machine stays responsive for its owner. A laptop chip with few
# performance cores slows down far more than linearly when every efficient core is busy too. When
# the core types cannot be read, a machine with a battery gets a quarter of its processors.
# OXIDESWARM_USB_CPU_INFO="total=N perf=N|- battery=0|1" replaces the probe (tests).
count_cpu_list() {
  n=0
  old_ifs=$IFS; IFS=,
  for part in $1; do
    case "$part" in
      *-*) a=${part%-*}; b=${part#*-}; n=$((n + b - a + 1)) ;;
      '') ;;
      *) n=$((n + 1)) ;;
    esac
  done
  IFS=$old_ifs
  printf '%s\n' "$n"
}
CPU_TOTAL=""; CPU_PERF=""; CPU_BATTERY=0
if [ -n "${OXIDESWARM_USB_CPU_INFO:-}" ]; then
  for kv in $OXIDESWARM_USB_CPU_INFO; do
    case "$kv" in
      total=*) CPU_TOTAL=${kv#total=} ;;
      perf=*) CPU_PERF=${kv#perf=} ;;
      battery=*) CPU_BATTERY=${kv#battery=} ;;
    esac
  done
elif [ "$OS" = Darwin ]; then
  CPU_TOTAL=$(sysctl -n hw.ncpu 2>/dev/null || true)
  CPU_PERF=$(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || true)
  if pmset -g batt 2>/dev/null | grep -q InternalBattery; then CPU_BATTERY=1; fi
else
  CPU_TOTAL=$(nproc 2>/dev/null || true)
  if [ -r /sys/devices/cpu_core/cpus ]; then CPU_PERF=$(count_cpu_list "$(cat /sys/devices/cpu_core/cpus)"); fi
  for b in /sys/class/power_supply/BAT*; do
    if [ -e "$b" ]; then CPU_BATTERY=1; break; fi
  done
fi
case "$CPU_TOTAL" in ''|*[!0-9]*) CPU_TOTAL=2 ;; esac
case "$CPU_PERF" in ''|*[!0-9]*) CPU_PERF="" ;; esac
[ "$CPU_TOTAL" -ge 1 ] || CPU_TOTAL=1
HALF=$((CPU_TOTAL / 2)); [ "$HALF" -ge 1 ] || HALF=1
if [ -n "$SLOTS_SET" ]; then
  SLOTS=$SLOTS_SET; SLOTS_WHY="set with --slots"
elif [ -n "$CPU_PERF" ] && [ "$CPU_PERF" -ge 1 ]; then
  SLOTS=$CPU_PERF; [ "$SLOTS" -le "$HALF" ] || SLOTS=$HALF
  SLOTS_WHY="performance cores, at most half of $CPU_TOTAL processors"
elif [ "$CPU_BATTERY" = 1 ]; then
  SLOTS=$((CPU_TOTAL / 4)); [ "$SLOTS" -ge 1 ] || SLOTS=1
  SLOTS_WHY="a quarter of $CPU_TOTAL processors on a machine with a battery"
else
  SLOTS=$HALF; SLOTS_WHY="half of $CPU_TOTAL processors"
fi
say "  slots:   $SLOTS ($SLOTS_WHY; change it with --slots N)"
WORKER_ARGS="worker --name $NAME --no-portal --max-concurrency $SLOTS"
if [ "$WORKSPACES" = 1 ] && command -v cargo >/dev/null 2>&1; then
  WORKER_ARGS="$WORKER_ARGS --workspaces"
fi
start_worker() {
  # shellcheck disable=SC2086
  nohup "$BIN" $WORKER_ARGS --isolation "$ISOLATION" >> "$LOG_DIR/worker.log" 2>&1 &
  echo $! > "$RUN_DIR/worker.pid"
}

# 6. Start it now; fall back to --isolation off where the OS cannot confine tasks.
say ""
say "Starting worker '$NAME' ..."
start_worker
sleep 4
if ! kill -0 "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null; then
  if [ "$ISOLATION" != off ] && [ "$EXPLICIT_ISOLATION" = 0 ]; then
    say "  the worker stopped with --isolation $ISOLATION (see $LOG_DIR/worker.log); trying --isolation off"
    ISOLATION=off
    start_worker
    sleep 3
  fi
  kill -0 "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || fail "the worker did not start with --isolation $ISOLATION; see $LOG_DIR/worker.log"
fi

# 7. Wait until the master lists it. The checks run with RUST_LOG=off: release 0.1.x writes its
# informational logs to standard output, and iroh logs an error line when a client exits.
CONNECTED=0
i=0
while [ $i -lt 30 ]; do
  # The JSON listing carries the real state; the plain listing also shows disconnected workers.
  if { if [ -n "$TICKET" ]; then RUST_LOG=off "$BIN" workers --json --master "$TICKET"; else RUST_LOG=off "$BIN" workers --json; fi; } 2>/dev/null | awk -v n="$NAME" '
      /"name":/ { v = $2; gsub(/[",]/, "", v); cur = v }
      /"state:connected"/ && cur == n { found = 1 }
      END { exit !found }'; then
    CONNECTED=1
    break
  fi
  sleep 1
  i=$((i + 1))
done
[ "$CONNECTED" = 1 ] || fail "the worker runs but the master does not list it yet; see $LOG_DIR/worker.log"
say "  connected (isolation: $ISOLATION)"
fi
# What an update keeps: the name, the build, the workspace choice and the isolation.
GPU_USED=0
if [ "$KIT_HAS_WORKER" = 1 ] && [ -n "$GPU_ARCHIVE" ] && [ "$ARCHIVE" = "$GPU_ARCHIVE" ]; then GPU_USED=1; fi
printf 'name=%s\ngpu=%s\nworkspaces=%s\nisolation=%s\nslots=%s\nworker=%s\nclient=%s\n' \
  "$NAME" "$GPU_USED" "$WORKSPACES" "$ISOLATION" "$SLOTS_SET" "$KIT_HAS_WORKER" "$KIT_HAS_CLIENT" > "$ROOT/kit-install.conf"

# 8. Start at login. The running worker is handed over to the autostart entry.
if [ "$KIT_HAS_WORKER" = 1 ]; then
if [ "$AUTOSTART" = 1 ]; then
  if [ "$OS" = Darwin ]; then
    mkdir -p "$HOME/Library/LaunchAgents"
    {
      printf '<?xml version="1.0" encoding="UTF-8"?>\n'
      printf '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
      printf '<plist version="1.0"><dict>\n'
      printf '  <key>Label</key><string>com.oxideswarm.usb-worker</string>\n'
      printf '  <key>ProgramArguments</key><array>\n'
      printf '    <string>%s</string>\n' "$BIN"
      for a in $WORKER_ARGS --isolation "$ISOLATION"; do printf '    <string>%s</string>\n' "$a"; done
      printf '  </array>\n'
      printf '  <key>EnvironmentVariables</key><dict><key>OXIDESWARM_HOME</key><string>%s</string></dict>\n' "$STATE"
      printf '  <key>WorkingDirectory</key><string>%s</string>\n' "$ROOT"
      printf '  <key>RunAtLoad</key><true/>\n'
      printf '  <key>KeepAlive</key><true/>\n'
      printf '  <key>StandardOutPath</key><string>%s</string>\n' "$LOG_DIR/worker.log"
      printf '  <key>StandardErrorPath</key><string>%s</string>\n' "$LOG_DIR/worker.log"
      printf '</dict></plist>\n'
    } > "$PLIST"
    CREATED_AUTOSTART_MAC=1
    kill "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/worker.pid"
    sleep 2
    launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    say "  autostart: LaunchAgent com.oxideswarm.usb-worker"
  elif command -v systemctl >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    UNIT_DIR=$(dirname "$UNIT")
    mkdir -p "$UNIT_DIR"
    {
      printf '[Unit]\nDescription=OxideSwarm worker (USB kit)\n\n[Service]\n'
      printf 'Environment=OXIDESWARM_HOME=%s\n' "$STATE"
      printf 'WorkingDirectory=%s\n' "$ROOT"
      printf 'ExecStart=%s %s --isolation %s\n' "$BIN" "$WORKER_ARGS" "$ISOLATION"
      printf 'Restart=on-failure\n\n[Install]\nWantedBy=default.target\n'
    } > "$UNIT_DIR/oxideswarm-usb-worker.service"
    CREATED_AUTOSTART_SYS=1
    kill "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/worker.pid"
    sleep 2
    systemctl --user daemon-reload
    systemctl --user enable --now oxideswarm-usb-worker.service
    say "  autostart: systemd user unit oxideswarm-usb-worker"
  else
    START_SCRIPT="$ROOT/start-worker.sh"
    {
      printf '#!/bin/sh\nset -eu\n'
      printf 'ROOT_DIR=$(cd "$(dirname "$0")" && pwd)\n'
      printf 'export OXIDESWARM_HOME="$ROOT_DIR/home"\n'
      printf 'RUN_DIR="$ROOT_DIR/run"\n'
      printf 'LOG_DIR="$ROOT_DIR/logs"\n'
      printf 'mkdir -p "$RUN_DIR" "$LOG_DIR"\n'
      printf 'if [ -f "$RUN_DIR/worker.pid" ]; then\n'
      printf '  OLD_PID=$(cat "$RUN_DIR/worker.pid" 2>/dev/null || true)\n'
      printf '  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then\n'
      printf '    case "$(ps -ww -p "$OLD_PID" -o command= 2>/dev/null || true)" in\n'
      printf '      *"$ROOT_DIR/bin/oxideswarm "*) exit 0 ;;\n'
      printf '    esac\n'
      printf '  fi\n'
      printf 'fi\n'
      printf 'cd "$ROOT_DIR"\n'
      printf 'nohup "$ROOT_DIR/bin/oxideswarm" %s --isolation %s >> "$LOG_DIR/worker.log" 2>&1 &\n' "$WORKER_ARGS" "$ISOLATION"
      printf 'echo $! > "$RUN_DIR/worker.pid"\n'
    } > "$START_SCRIPT"
    chmod 755 "$START_SCRIPT"
    XDG_AUTO_DIR=$(dirname "$XDG_DESKTOP")
    mkdir -p "$XDG_AUTO_DIR"
    {
      printf '[Desktop Entry]\n'
      printf 'Type=Application\n'
      printf 'Version=1.0\n'
      printf 'Name=OxideSwarm Worker\n'
      printf 'Comment=Start OxideSwarm USB worker at login\n'
      printf 'Exec="%s"\n' "$START_SCRIPT"
      printf 'Terminal=false\n'
      printf 'X-GNOME-Autostart-enabled=true\n'
    } > "$XDG_DESKTOP"
    CREATED_AUTOSTART_XDG="$XDG_DESKTOP"
    say "  autostart: XDG entry $XDG_DESKTOP (worker PID $(cat "$RUN_DIR/worker.pid"))"
  fi
else
  say "  autostart: skipped (--no-autostart); worker PID $(cat "$RUN_DIR/worker.pid")"
fi
fi

ROLLBACK_ARMED=0

if [ "$UPDATE" = 1 ]; then
  say ""
  if [ "$KIT_HAS_WORKER" = 1 ]; then
    say "Updated. Worker '$NAME' runs $("$BIN" --version) from $ROOT."
  else
    say "Updated. Machine '$NAME' runs $("$BIN" --version) from $ROOT."
  fi
  exit 0
fi

# 9. Check the cluster.
if [ "$KIT_HAS_CLIENT" = 1 ]; then
  say ""
  say "Checking the cluster (oxideswarm doctor) ..."
  if { if [ -n "$TICKET" ]; then RUST_LOG=off "$BIN" doctor --master "$TICKET"; else RUST_LOG=off "$BIN" doctor; fi; }; then
    DOCTOR="passed"
  else
    DOCTOR="reported a problem (see above)"
  fi

  say ""
  if [ "$KIT_HAS_WORKER" = 1 ]; then
    say "Done. Worker '$NAME' is part of the cluster; the doctor check $DOCTOR."
    say "Files: $ROOT   Log: $LOG_DIR/worker.log"
  else
    say "Done. Machine '$NAME' is enrolled as a client; the doctor check $DOCTOR."
    say "Files: $ROOT   Log: $INSTALL_LOG_ACTIVE"
  fi
else
  say ""
  say "Skipping doctor check (worker-only kit; roles do not include 'client')."
  say "Done. Worker '$NAME' is part of the cluster (doctor skipped: worker-only kit)."
  say "Files: $ROOT   Log: $LOG_DIR/worker.log"
fi
say "To remove it, run uninstall from the stick."
