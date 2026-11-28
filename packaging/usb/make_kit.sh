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
#   --isolation MODE bake the default worker isolation mode (`off` or `fs`) into the kit
#   --gpu            also put the `-gpu` archives (built with gpu-wgpu) on the kit; the installers ask
#                    whether this machine's GPU should do work and then install that build
#   --toolset LIST   bundle pinned Windows developer tools (`full` or comma list of python,rust,mingw,git)
#   --tools-dir DIR  take pinned tool files from DIR instead of downloading them
#
# Compatible with bash 3.2 (macOS /bin/bash).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
DEST=""
VERSION=""
ARCHIVES=""
REPLACE=0
NO_AUTOSTART=0
GPU=0
ISOLATION=""
TOOLSET=""
TOOLS_DIR=""
TOOLS_LOCK="${OXIDESWARM_TOOLS_LOCK:-$HERE/tools.lock}"
REPO="DuongNAD/OxideSwarm"
TARGETS="aarch64-apple-darwin.tar.gz x86_64-apple-darwin.tar.gz x86_64-pc-windows-msvc.zip x86_64-unknown-linux-gnu.tar.gz"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print tolower($1)}'
  else
    sha256sum "$1" | awk '{print tolower($1)}'
  fi
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) [ $# -ge 2 ] || fail "--version needs a value"; VERSION=${2#v}; shift ;;
    --archives) [ $# -ge 2 ] || fail "--archives needs a value"; ARCHIVES=$2; shift ;;
    --replace) REPLACE=1 ;;
    --no-autostart) NO_AUTOSTART=1 ;;
    --gpu) GPU=1 ;;
    --isolation)
      [ $# -ge 2 ] || fail "--isolation needs a value (off or fs)"
      case "$2" in
        off|fs) ISOLATION=$2 ;;
        *) fail "--isolation must be off or fs (got: $2)" ;;
      esac
      shift ;;
    --toolset)
      [ $# -ge 2 ] || fail "--toolset needs a value (full or comma list of python,rust,mingw,git)"
      TOOLSET=$2
      shift ;;
    --tools-dir)
      [ $# -ge 2 ] || fail "--tools-dir needs a value"
      TOOLS_DIR=$2
      shift ;;
    --tools-lock)
      [ $# -ge 2 ] || fail "--tools-lock needs a value"
      TOOLS_LOCK=$2
      shift ;;
    -h|--help) sed -n '2,23p' "$0"; exit 0 ;;
    -*) fail "unknown option: $1" ;;
    *) [ -z "$DEST" ] || fail "only one destination folder"; DEST=$1 ;;
  esac
  shift
done
[ -n "$DEST" ] || fail "usage: ... | bash make_kit.sh <DIR> [--version V] [--archives DIR] [--replace] [--no-autostart] [--isolation off|fs] [--gpu] [--toolset full|LIST] [--tools-dir DIR]"
[ -d "$DEST" ] || fail "$DEST is not a folder"
if [ -n "$TOOLS_DIR" ]; then
  [ -d "$TOOLS_DIR" ] || fail "--tools-dir $TOOLS_DIR is not a folder"
fi
SELECTED_TOOLS=""
if [ -n "$TOOLSET" ]; then
  [ -f "$TOOLS_LOCK" ] || fail "tools lock not found: $TOOLS_LOCK"
  _want_python=0
  _want_rust=0
  _want_mingw=0
  _want_git=0
  _old_ifs=$IFS
  IFS=,
  for _t in $TOOLSET; do
    _t_clean=$(printf '%s' "$_t" | tr -d ' \t\r\n' | tr '[:upper:]' '[:lower:]')
    case "$_t_clean" in
      full) _want_python=1; _want_rust=1; _want_mingw=1; _want_git=1 ;;
      python) _want_python=1 ;;
      rust) _want_rust=1 ;;
      mingw) _want_mingw=1 ;;
      git) _want_git=1 ;;
      '') fail "--toolset cannot contain empty entries" ;;
      *) fail "unknown tool '$_t' in --toolset (expected full or comma list of python,rust,mingw,git)" ;;
    esac
  done
  IFS=$_old_ifs
  [ "$_want_python" = 1 ] && SELECTED_TOOLS="$SELECTED_TOOLS python"
  [ "$_want_rust" = 1 ] && SELECTED_TOOLS="$SELECTED_TOOLS rust"
  [ "$_want_mingw" = 1 ] && SELECTED_TOOLS="$SELECTED_TOOLS mingw"
  [ "$_want_git" = 1 ] && SELECTED_TOOLS="$SELECTED_TOOLS git"
  SELECTED_TOOLS=${SELECTED_TOOLS# }
  [ -n "$SELECTED_TOOLS" ] || fail "--toolset resolved to no tools"
fi
# The release names a gpu-wgpu build <name>-<target>-gpu.<ext>, next to the plain one.
if [ "$GPU" = 1 ]; then
  for t in $TARGETS; do
    case "$t" in
      *.tar.gz) TARGETS="$TARGETS ${t%.tar.gz}-gpu.tar.gz" ;;
      *.zip) TARGETS="$TARGETS ${t%.zip}-gpu.zip" ;;
    esac
  done
fi
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
         "Install OxideSwarm.cmd" "Uninstall OxideSwarm.cmd" \
         "Update OxideSwarm.command" "Update OxideSwarm.cmd"; do
  copy "$HERE/$f" "$KIT/$f"
done
copy "$HERE/../../windows/revoke-isolation-acls.ps1" "$KIT/revoke-isolation-acls.ps1"
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
  actual=$(sha256_file "$KIT/archives/$name")
  [ "$expected" = "$actual" ] || fail "checksum mismatch for $name"
done

if [ -n "$SELECTED_TOOLS" ]; then
  mkdir -p "$KIT/tools/check/cc-check/c" "$KIT/tools/check/cc-check/src"
  copy "$HERE/tools/check/hello.rs" "$KIT/tools/check/hello.rs"
  copy "$HERE/tools/check/cc-check/Cargo.toml" "$KIT/tools/check/cc-check/Cargo.toml"
  copy "$HERE/tools/check/cc-check/build.rs" "$KIT/tools/check/cc-check/build.rs"
  copy "$HERE/tools/check/cc-check/c/add.c" "$KIT/tools/check/cc-check/c/add.c"
  copy "$HERE/tools/check/cc-check/src/main.rs" "$KIT/tools/check/cc-check/src/main.rs"

  # Ensure SHA256SUMS ends with a newline before appending tools/<file> entries.
  if [ -s "$KIT/archives/SHA256SUMS" ]; then
    _last_byte=$(tail -c 1 "$KIT/archives/SHA256SUMS" | wc -l | tr -d ' ')
    [ "$_last_byte" = 1 ] || printf '\n' >> "$KIT/archives/SHA256SUMS"
  fi

  : > "$KIT/tools/tools.lock"
  _tools_cache="${OXIDESWARM_TOOLS_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/oxideswarm/usb-tools}"
  [ -n "$TOOLS_DIR" ] || mkdir -p "$_tools_cache"

  _py_ver=""
  _rust_ver=""
  _mingw_ver=""
  _git_ver=""
  _matched_count=0

  while IFS="$(printf '\t')" read -r _l_tool _l_comp _l_ver _l_file _l_sha _l_url; do
    case "$_l_tool" in
      ''|\#*) continue ;;
    esac
    [ -n "$_l_url" ] || fail "malformed line in $TOOLS_LOCK for tool '$_l_tool'"
    case " $SELECTED_TOOLS " in
      *" $_l_tool "*) ;;
      *) continue ;;
    esac
    case "$_l_file" in
      ''|*/*|*\\*|*..*) fail "unsafe filename '$_l_file' in $TOOLS_LOCK" ;;
    esac
    _l_sha=$(printf '%s' "$_l_sha" | tr -d ' \r\n\t' | tr '[:upper:]' '[:lower:]')
    case "$_l_sha" in
      ????????????????????????????????????????????????????????????????)
        case "$_l_sha" in
          *[!0-9a-f]*) fail "invalid sha256 '$_l_sha' in $TOOLS_LOCK for $_l_file" ;;
        esac ;;
      *) fail "invalid sha256 '$_l_sha' in $TOOLS_LOCK for $_l_file" ;;
    esac

    if [ -n "$TOOLS_DIR" ]; then
      _src="$TOOLS_DIR/$_l_file"
      [ -f "$_src" ] || fail "missing tool file in --tools-dir: $_src"
    else
      _src="$_tools_cache/$_l_file"
      _need_dl=1
      if [ -f "$_src" ]; then
        _cached_sha=$(sha256_file "$_src")
        if [ "$_cached_sha" = "$_l_sha" ]; then
          _need_dl=0
        fi
      fi
      if [ "$_need_dl" = 1 ]; then
        printf 'downloading %s\n' "$_l_file"
        _tmp_dl="$_tools_cache/.$_l_file.tmp.$$"
        rm -f "$_tmp_dl"
        curl -fsSL -o "$_tmp_dl" "$_l_url" || { rm -f "$_tmp_dl"; fail "cannot download $_l_url"; }
        _dl_sha=$(sha256_file "$_tmp_dl")
        if [ "$_dl_sha" != "$_l_sha" ]; then
          rm -f "$_tmp_dl"
          fail "checksum mismatch for downloaded $_l_file (expected $_l_sha, got $_dl_sha)"
        fi
        mv -f "$_tmp_dl" "$_src"
      fi
    fi

    _actual_sha=$(sha256_file "$_src")
    [ "$_actual_sha" = "$_l_sha" ] || fail "checksum mismatch for $_l_file (expected $_l_sha, got $_actual_sha)"
    copy "$_src" "$KIT/tools/$_l_file"
    printf '%s  tools/%s\n' "$_l_sha" "$_l_file" >> "$KIT/archives/SHA256SUMS"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$_l_tool" "$_l_comp" "$_l_ver" "$_l_file" "$_l_sha" "$_l_url" >> "$KIT/tools/tools.lock"
    _matched_count=$((_matched_count + 1))

    case "$_l_tool" in
      python) _py_ver=$_l_ver ;;
      rust) _rust_ver=$_l_ver ;;
      mingw) _mingw_ver=$_l_ver ;;
      git) _git_ver=$_l_ver ;;
    esac
  done < <(tr -d '\r' < "$TOOLS_LOCK")

  [ "$_matched_count" -ge 1 ] || fail "no entries in $TOOLS_LOCK matched --toolset ($SELECTED_TOOLS)"
  for _req_t in $SELECTED_TOOLS; do
    case "$_req_t" in
      python) [ -n "$_py_ver" ] || fail "missing 'python' entry in $TOOLS_LOCK" ;;
      rust) [ -n "$_rust_ver" ] || fail "missing 'rust' entry in $TOOLS_LOCK" ;;
      mingw) [ -n "$_mingw_ver" ] || fail "missing 'mingw' entry in $TOOLS_LOCK" ;;
      git) [ -n "$_git_ver" ] || fail "missing 'git' entry in $TOOLS_LOCK" ;;
    esac
  done

  # NOTICE.txt (design 0110 D6): required for any toolset kit, naming exact versions, licences & GPL source releases.
  if [ -n "${OXIDESWARM_TOOLS_NOTICE_FILE+x}" ]; then
    [ -f "$OXIDESWARM_TOOLS_NOTICE_FILE" ] || fail "OXIDESWARM_TOOLS_NOTICE_FILE does not exist: $OXIDESWARM_TOOLS_NOTICE_FILE"
    copy "$OXIDESWARM_TOOLS_NOTICE_FILE" "$KIT/tools/NOTICE.txt"
  else
    {
      printf 'OxideSwarm Windows USB kit developer toolset notices (design 0110 D6)\n'
      printf '====================================================================\n\n'
      if [ -n "$_py_ver" ]; then
        _py_nodot=$(printf '%s' "$_py_ver" | tr -d '.')
        printf 'Python %s (Windows embeddable amd64)\n' "$_py_ver"
        printf '  Licence: Python Software Foundation License (PSF License; LICENSE.txt in package)\n'
        printf '  Release: https://www.python.org/downloads/release/python-%s/\n\n' "$_py_nodot"
      fi
      if [ -n "$_rust_ver" ]; then
        printf 'Rust %s (x86_64-pc-windows-gnu: rustc, rust-std, cargo, rust-mingw)\n' "$_rust_ver"
        printf '  Licence: MIT OR Apache-2.0 (rust-mingw carries MinGW-w64 runtime files under their licences)\n'
        printf '  Release: https://static.rust-lang.org/dist/channel-rust-%s.toml\n' "$_rust_ver"
        printf '  Source:  https://github.com/rust-lang/rust/releases/tag/%s\n\n' "$_rust_ver"
      fi
      if [ -n "$_mingw_ver" ]; then
        printf 'w64devkit %s x64 (MinGW-w64 C/GCC toolchain)\n' "$_mingw_ver"
        printf '  Licence: GNU GPL v3 with GCC Runtime Library Exception (GCC & binutils), MinGW-w64 headers/runtime, BusyBox GNU GPL v2\n'
        printf '  Release: https://github.com/skeeto/w64devkit/releases/tag/v%s\n' "$_mingw_ver"
        printf '  Source:  https://github.com/skeeto/w64devkit/releases/download/v%s/source.tar\n\n' "$_mingw_ver"
      fi
      if [ -n "$_git_ver" ]; then
        printf 'MinGit %s 64-bit (Git for Windows)\n' "$_git_ver"
        printf '  Licence: GNU GPL v2 (and bundled components)\n'
        printf '  Release: https://github.com/git-for-windows/git/releases/tag/v%s.windows.1\n' "$_git_ver"
        printf '  Source:  https://github.com/git-for-windows/git/archive/refs/tags/v%s.windows.1.tar.gz\n\n' "$_git_ver"
      fi
    } > "$KIT/tools/NOTICE.txt"
  fi
  [ -s "$KIT/tools/NOTICE.txt" ] || fail "toolset kit requires a non-empty tools/NOTICE.txt"

  _gpu_json=false
  [ "$GPU" = 1 ] && _gpu_json=true
  _toolset_json=""
  for _req_t in $SELECTED_TOOLS; do
    if [ -n "$_toolset_json" ]; then
      _toolset_json="$_toolset_json, \"$_req_t\""
    else
      _toolset_json="\"$_req_t\""
    fi
  done
  cat > "$KIT/kit.json" <<EOF
{
  "kit_format": 0,
  "version": "$VERSION",
  "roles": ["worker", "client"],
  "workspaces": true,
  "gpu": $_gpu_json,
  "toolset": [$_toolset_json]
}
EOF
fi

(umask 077; printf '%s\n' "$TOKEN" > "$KIT/join.token")
if [ "$NO_AUTOSTART" = 1 ]; then
  printf 'The installers on this kit add no start at login.\n' > "$KIT/no-autostart"
fi
if [ -n "$ISOLATION" ]; then
  printf '%s\n' "$ISOLATION" > "$KIT/isolation"
fi
sync 2>/dev/null || true
printf 'Kit written to %s (OxideSwarm %s).\n' "$KIT" "$VERSION"
printf 'The stick now holds a join token: keep it with people you trust.\n'
