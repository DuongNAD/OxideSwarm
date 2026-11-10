#!/bin/bash
# OxideSwarm USB installer for macOS and Linux (kit format v0, design 0053 prototype).
#
# Installs the worker for the current user from this USB stick, joins the cluster with the kit's
# join token, starts the worker, and starts it again at every login. Nothing outside the install
# folder is changed except the per-user autostart entry. The user's own ~/.oxideswarm is never
# read or written: the installed worker keeps its state in <install folder>/home.
#
#   bash install.sh [--name NAME] [--install-root DIR] [--no-autostart] [--no-workspaces] [--gpu|--no-gpu]
#
# Compatible with bash 3.2 (macOS /bin/bash).
set -eu

KIT_DIR=$(cd "$(dirname "$0")" && pwd)
NAME=""
ROOT=""
AUTOSTART=1
WORKSPACES=1
GPU=""

say() { printf '%s\n' "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --name) [ $# -ge 2 ] || fail "--name needs a value"; NAME=$2; shift ;;
    --install-root) [ $# -ge 2 ] || fail "--install-root needs a value"; ROOT=$2; shift ;;
    --no-autostart) AUTOSTART=0 ;;
    --no-workspaces) WORKSPACES=0 ;;
    --gpu) [ "$GPU" = 0 ] && fail "--gpu and --no-gpu cannot be used together"; GPU=1 ;;
    --no-gpu) [ "$GPU" = 1 ] && fail "--gpu and --no-gpu cannot be used together"; GPU=0 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
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

[ -f "$KIT_DIR/join.token" ] || fail "join.token is missing from the kit"
# A kit made with make_kit.sh --no-autostart is meant for a trial on someone else's machine.
[ -f "$KIT_DIR/no-autostart" ] && AUTOSTART=0
[ -f "$KIT_DIR/archives/SHA256SUMS" ] || fail "archives/SHA256SUMS is missing from the kit"

ARCHIVE=""
for f in "$KIT_DIR"/archives/oxideswarm-*-"$TARGET".tar.gz; do
  [ -f "$f" ] && ARCHIVE=$f
done
[ -n "$ARCHIVE" ] || fail "no archive for $TARGET in $KIT_DIR/archives"
# A kit made with make_kit.sh --gpu also holds the gpu-wgpu build, which uses this machine's GPU
# when it finds one. It is installed only when the person at the keyboard agrees (or --gpu).
GPU_ARCHIVE=""
for f in "$KIT_DIR"/archives/oxideswarm-*-"$TARGET"-gpu.tar.gz; do
  [ -f "$f" ] && GPU_ARCHIVE=$f
done
[ "$GPU" = 1 ] && [ -z "$GPU_ARCHIVE" ] && fail "this kit has no GPU build for $TARGET (make it with make_kit.sh --gpu)"

say "OxideSwarm USB installer"
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

# 1. Check the archive against the kit's SHA256SUMS. This catches a corrupted or outdated copy;
#    it cannot catch a deliberate swap of the whole stick (design 0053, Security).
EXPECTED=$(awk -v n="$ARCHIVE_NAME" '$2 == n {print $1}' "$KIT_DIR/archives/SHA256SUMS")
[ -n "$EXPECTED" ] || fail "$ARCHIVE_NAME is not listed in SHA256SUMS"
if command -v shasum >/dev/null 2>&1; then
  ACTUAL=$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')
else
  ACTUAL=$(sha256sum "$ARCHIVE" | awk '{print $1}')
fi
[ "$EXPECTED" = "$ACTUAL" ] || fail "checksum mismatch for $ARCHIVE_NAME: the copy on the stick is damaged"
say "  archive: $ARCHIVE_NAME (checksum OK)"

# 2. Unpack the binaries.
BIN_DIR="$ROOT/bin"
STATE="$ROOT/home"
RUN_DIR="$ROOT/run"
LOG_DIR="$ROOT/logs"
mkdir -p "$BIN_DIR" "$STATE" "$RUN_DIR" "$LOG_DIR"
chmod 700 "$STATE"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/oxideswarm-usb.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
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
  if [ -n "$OLD" ] && kill -0 "$OLD" 2>/dev/null; then
    say "  stopping the worker started earlier (PID $OLD)"
    kill "$OLD" 2>/dev/null || true
    sleep 2
  fi
  rm -f "$RUN_DIR/worker.pid"
fi

# 4. Join: the token is read from standard input only, never from the command line.
say ""
say "Joining the cluster ..."
if ! "$BIN" join --no-doctor < "$KIT_DIR/join.token"; then
  fail "join failed (see the message above); nothing was started"
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
# Use at most half of the CPU cores, so the machine stays responsive for its owner.
if [ "$OS" = Darwin ]; then CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 2); else CORES=$(nproc 2>/dev/null || echo 2); fi
SLOTS=$((CORES / 2)); [ "$SLOTS" -ge 1 ] || SLOTS=1
WORKER_ARGS="worker --name $NAME --no-portal --max-concurrency $SLOTS"
if [ "$WORKSPACES" = 1 ] && command -v cargo >/dev/null 2>&1; then
  WORKER_ARGS="$WORKER_ARGS --workspaces"
fi
ISOLATION=fs
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
  say "  the worker stopped with --isolation fs (see $LOG_DIR/worker.log); trying --isolation off"
  ISOLATION=off
  start_worker
  sleep 3
  kill -0 "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || fail "the worker did not start; see $LOG_DIR/worker.log"
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

# 8. Start at login. The running worker is handed over to the autostart entry.
if [ "$AUTOSTART" = 1 ]; then
  if [ "$OS" = Darwin ]; then
    PLIST="$HOME/Library/LaunchAgents/com.oxideswarm.usb-worker.plist"
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
    kill "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/worker.pid"
    sleep 2
    launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    say "  autostart: LaunchAgent com.oxideswarm.usb-worker"
  elif command -v systemctl >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
    mkdir -p "$UNIT_DIR"
    {
      printf '[Unit]\nDescription=OxideSwarm worker (USB kit)\n\n[Service]\n'
      printf 'Environment=OXIDESWARM_HOME=%s\n' "$STATE"
      printf 'WorkingDirectory=%s\n' "$ROOT"
      printf 'ExecStart=%s %s --isolation %s\n' "$BIN" "$WORKER_ARGS" "$ISOLATION"
      printf 'Restart=on-failure\n\n[Install]\nWantedBy=default.target\n'
    } > "$UNIT_DIR/oxideswarm-usb-worker.service"
    kill "$(cat "$RUN_DIR/worker.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/worker.pid"
    sleep 2
    systemctl --user daemon-reload
    systemctl --user enable --now oxideswarm-usb-worker.service
    say "  autostart: systemd user unit oxideswarm-usb-worker"
  else
    say "  autostart: not available here; the worker runs until you log out (PID $(cat "$RUN_DIR/worker.pid"))"
  fi
else
  say "  autostart: skipped (--no-autostart); worker PID $(cat "$RUN_DIR/worker.pid")"
fi

# 9. Check the cluster.
say ""
say "Checking the cluster (oxideswarm doctor) ..."
if { if [ -n "$TICKET" ]; then RUST_LOG=off "$BIN" doctor --master "$TICKET"; else RUST_LOG=off "$BIN" doctor; fi; }; then
  DOCTOR="passed"
else
  DOCTOR="reported a problem (see above)"
fi

say ""
say "Done. Worker '$NAME' is part of the cluster; the doctor check $DOCTOR."
say "Files: $ROOT   Log: $LOG_DIR/worker.log"
say "To remove it, run uninstall from the stick."
