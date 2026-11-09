#!/bin/bash
# Writes an OxideSwarm setup kit (kit format v0) to a USB stick or any folder.
#
#   oxideswarm secret join-token --master <MASTER_IP>:8088 | bash packaging/usb/make_kit.sh <DIR> [options]
#
# The join token is read from standard input only (never from the command line). The kit holds
# the release archives for macOS, Windows and Linux with SHA256SUMS, the installers of this
# folder, and the token. See packaging/usb/README.md.
#
# Options:
#   --version V      release to put on the kit (default: the version of `oxideswarm --version`)
#   --archives DIR   take the archives and SHA256SUMS from DIR instead of downloading them
#   --replace        replace an existing <DIR>/OxideSwarm folder
#   --no-autostart   the installers on this kit add no start at login (a trial on someone else's machine)
#
# Compatible with bash 3.2 (macOS /bin/bash).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
DEST=""
VERSION=""
ARCHIVES=""
REPLACE=0
NO_AUTOSTART=0
REPO="DuongNAD/OxideSwarm"
TARGETS="aarch64-apple-darwin.tar.gz x86_64-apple-darwin.tar.gz x86_64-pc-windows-msvc.zip x86_64-unknown-linux-gnu.tar.gz"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version) [ $# -ge 2 ] || fail "--version needs a value"; VERSION=${2#v}; shift ;;
    --archives) [ $# -ge 2 ] || fail "--archives needs a value"; ARCHIVES=$2; shift ;;
    --replace) REPLACE=1 ;;
    --no-autostart) NO_AUTOSTART=1 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    -*) fail "unknown option: $1" ;;
    *) [ -z "$DEST" ] || fail "only one destination folder"; DEST=$1 ;;
  esac
  shift
done
[ -n "$DEST" ] || fail "usage: ... | bash make_kit.sh <DIR> [--version V] [--archives DIR] [--replace]"
[ -d "$DEST" ] || fail "$DEST is not a folder"
[ -t 0 ] && fail "pipe the join token in: oxideswarm secret join-token --master <MASTER_IP>:8088 | bash make_kit.sh $DEST"

TOKEN=$(head -c 8192)
TOKEN=$(printf '%s' "$TOKEN" | tr -d ' \r\n\t')
case "$TOKEN" in
  oxs1-join:*) ;;
  *) fail "standard input is not a join token (it must start with oxs1-join:)" ;;
esac

if [ -z "$VERSION" ]; then
  command -v oxideswarm >/dev/null 2>&1 || fail "pass --version (no oxideswarm on PATH to ask)"
  VERSION=$(oxideswarm --version | awk '{print $NF}')
fi

KIT="$DEST/OxideSwarm"
if [ -e "$KIT" ]; then
  [ "$REPLACE" = 1 ] || fail "$KIT already exists (use --replace)"
  [ -L "$KIT" ] && fail "$KIT is a symbolic link; not touching it"
  [ -f "$KIT/install.sh" ] || fail "$KIT does not look like a kit; not replacing it"
  rm -rf "$KIT"
fi
mkdir -p "$KIT/archives"

# Copy file contents only, so no extended attributes (and no ._ files on exFAT) come along.
copy() { cat "$1" > "$2"; }
for f in install.sh uninstall.sh install.ps1 uninstall.ps1 \
         "Install OxideSwarm.command" "Uninstall OxideSwarm.command" \
         "Install OxideSwarm.cmd" "Uninstall OxideSwarm.cmd"; do
  copy "$HERE/$f" "$KIT/$f"
done
copy "$HERE/KIT_README.txt" "$KIT/README.txt"
chmod 755 "$KIT"/*.sh "$KIT"/*.command 2>/dev/null || true

if [ -n "$ARCHIVES" ]; then
  copy "$ARCHIVES/SHA256SUMS" "$KIT/archives/SHA256SUMS"
  for t in $TARGETS; do
    copy "$ARCHIVES/oxideswarm-$VERSION-$t" "$KIT/archives/oxideswarm-$VERSION-$t"
  done
else
  BASE="https://github.com/$REPO/releases/download/v$VERSION"
  curl -fsSL -o "$KIT/archives/SHA256SUMS" "$BASE/SHA256SUMS" || fail "cannot download $BASE/SHA256SUMS"
  for t in $TARGETS; do
    printf 'downloading oxideswarm-%s-%s\n' "$VERSION" "$t"
    curl -fsSL -o "$KIT/archives/oxideswarm-$VERSION-$t" "$BASE/oxideswarm-$VERSION-$t" \
      || fail "cannot download oxideswarm-$VERSION-$t"
  done
fi

for t in $TARGETS; do
  name="oxideswarm-$VERSION-$t"
  expected=$(awk -v n="$name" '$2 == n {print $1}' "$KIT/archives/SHA256SUMS")
  [ -n "$expected" ] || fail "$name is not listed in SHA256SUMS"
  if command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$KIT/archives/$name" | awk '{print $1}')
  else
    actual=$(sha256sum "$KIT/archives/$name" | awk '{print $1}')
  fi
  [ "$expected" = "$actual" ] || fail "checksum mismatch for $name"
done

(umask 077; printf '%s\n' "$TOKEN" > "$KIT/join.token")
if [ "$NO_AUTOSTART" = 1 ]; then
  printf 'The installers on this kit add no start at login.\n' > "$KIT/no-autostart"
fi
sync 2>/dev/null || true
printf 'Kit written to %s (OxideSwarm %s).\n' "$KIT" "$VERSION"
printf 'The stick now holds a join token: keep it with people you trust.\n'
