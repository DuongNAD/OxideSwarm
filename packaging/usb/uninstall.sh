#!/bin/bash
# OxideSwarm USB uninstaller for macOS and Linux (kit format v0).
# Stops the worker this kit installed, removes its autostart entry and its folder (binaries, the
# cluster secret it received, logs). Nothing else is touched.
#   bash uninstall.sh [--install-root DIR] [--yes]
set -eu
ROOT=""
YES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --install-root) ROOT=$2; shift ;;
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
  printf 'Remove the OxideSwarm worker installed in %s? [y/N] ' "$ROOT"
  read -r ans
  case "$ans" in y|Y|yes|YES) ;; *) printf 'Nothing removed.\n'; exit 0 ;; esac
fi
if [ "$OS" = Darwin ]; then
  launchctl bootout "gui/$(id -u)/com.oxideswarm.usb-worker" 2>/dev/null || true
  rm -f "$HOME/Library/LaunchAgents/com.oxideswarm.usb-worker.plist"
elif command -v systemctl >/dev/null 2>&1; then
  systemctl --user disable --now oxideswarm-usb-worker.service 2>/dev/null || true
  rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/oxideswarm-usb-worker.service"
  systemctl --user daemon-reload 2>/dev/null || true
fi
if [ -f "$ROOT/run/worker.pid" ]; then
  PID=$(cat "$ROOT/run/worker.pid" 2>/dev/null || true)
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  sleep 1
fi
rm -rf "$ROOT"
printf 'Removed. The cluster owner can also remove this machine from the cluster.\n'
