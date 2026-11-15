#!/bin/bash
# OxideSwarm USB uninstaller for macOS and Linux (kit format v0 / v1).
# Stops the worker this kit installed, removes its autostart entry and its folder (binaries, the
# credentials it received, logs). Nothing else is touched.
#   bash uninstall.sh [--install-root DIR] [--keep-key] [--yes]
set -eu
ROOT=""
YES=0
KEEP_KEY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --install-root)
      [ $# -ge 2 ] || { printf 'ERROR: --install-root needs a value\n' >&2; exit 2; }
      ROOT=$2; shift ;;
    --keep-key) KEEP_KEY=1 ;;
    --yes) YES=1 ;;
    *) printf 'ERROR: unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done
OS=$(uname -s)
if [ -z "$ROOT" ]; then
  if [ "$OS" = Darwin ]; then ROOT="$HOME/Library/Application Support/OxideSwarm"
  else ROOT="${XDG_DATA_HOME:-$HOME/.local/share}/oxideswarm"; fi
fi
[ -d "$ROOT" ] || { printf 'Nothing to remove: %s does not exist.\n' "$ROOT"; exit 0; }
[ -f "$ROOT/bin/oxideswarm" ] || { printf 'ERROR: %s does not look like a USB-kit install; not touching it.\n' "$ROOT" >&2; exit 1; }
if [ "$YES" != 1 ]; then
  printf 'Remove the OxideSwarm install in %s? [y/N] ' "$ROOT"
  read -r ans
  case "$ans" in y|Y|yes|YES) ;; *) printf 'Nothing removed.\n'; exit 0 ;; esac
  if [ "$KEEP_KEY" != 1 ] && [ -f "$ROOT/home/node.key" ]; then
    printf 'Keep machine identity key (%s/home/node.key) so rejoining keeps the same identity? [y/N] ' "$ROOT"
    read -r keep_ans
    case "$keep_ans" in y|Y|yes|YES) KEEP_KEY=1 ;; esac
  fi
fi
if [ "$OS" = Darwin ]; then
  launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
  rm -f "$HOME/Library/LaunchAgents/com.oxideswarm.usb-worker.plist"
else
  if command -v systemctl >/dev/null 2>&1; then
    systemctl --user disable --now oxideswarm-usb-worker.service 2>/dev/null || true
    rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/oxideswarm-usb-worker.service"
    systemctl --user daemon-reload 2>/dev/null || true
  fi
  XDG_DESKTOP="${XDG_CONFIG_HOME:-$HOME/.config}/autostart/oxideswarm-usb-worker.desktop"
  if [ -f "$XDG_DESKTOP" ] && grep -qF "$ROOT" "$XDG_DESKTOP" 2>/dev/null; then
    rm -f "$XDG_DESKTOP"
  fi
fi
if [ -f "$ROOT/run/worker.pid" ]; then
  PID=$(cat "$ROOT/run/worker.pid" 2>/dev/null || true)
  # Only a process that runs this install's binary (a PID file can outlive a reboot).
  case "$(ps -ww -p "$PID" -o command= 2>/dev/null || true)" in
    *"$ROOT/bin/oxideswarm "*) kill "$PID" 2>/dev/null || true ;;
  esac
  sleep 1
fi
if [ "$KEEP_KEY" = 1 ] && [ -f "$ROOT/home/node.key" ]; then
  rm -f "$ROOT/kit-install.conf" "$ROOT/start-worker.sh"
  rm -rf "${ROOT:?}/bin" "${ROOT:?}/run" "${ROOT:?}/logs" "${ROOT:?}/workspaces"
  for f in "$ROOT/home/"*; do
    [ -e "$f" ] || continue
    [ "$(basename "$f")" = "node.key" ] || rm -rf "$f"
  done
  printf 'Removed install; kept machine identity key at %s/home/node.key.\n' "$ROOT"
else
  rm -rf "$ROOT"
  printf 'Removed. The cluster owner can also remove this machine from the cluster.\n'
fi
