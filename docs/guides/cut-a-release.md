# Cut a release

## Goal

Build, tag, verify, and publish a versioned OxideSwarm release for Linux, macOS, and Windows from a clean `master` merge commit, with verified `SHA256SUMS`, GitHub provenance attestations, and passing macOS and Windows smoke checks.

## Prerequisites

- Maintainer access on [DuongNAD/OxideSwarm](https://github.com/DuongNAD/OxideSwarm). Today only the owner holds merge rights to protected `master` and tag-push rights (`LV-15`; see [Accounts and secrets](#accounts-and-secrets)).
- Git, GitHub CLI (`gh`, authenticated with `repo` and `workflow` scope), and the Rust toolchain matching `rust-version` in [Cargo.toml](../../Cargo.toml).
- For archive checks: `shasum` (`macOS`) or `sha256sum` (`Linux`), and PowerShell (`Get-FileHash`, `Expand-Archive`) on `Windows`.
- A test machine on macOS (`aarch64-apple-darwin` or `x86_64-apple-darwin`) and a test machine on Windows (`x86_64-pc-windows-msvc`) to run the release smoke checks.
- An isolated temporary directory on each test machine so checks never touch an existing cluster, `~/.oxideswarm`, or ports `8088` and `8080`.

## Steps

### 1. Check the wire version and release-train rule

Compare `origin/master` against the previous release tag `v<PREV>` to see whether any merged change touched [crates/core/src/protocol.rs](../../crates/core/src/protocol.rs) (`PROTOCOL_VERSION_CURRENT`, `PROTOCOL_VERSION_MIN_COMPATIBLE`) or [crates/master/src/store.rs](../../crates/master/src/store.rs) (`STORE_FORMAT_VERSION`):

- **No wire change (`0.4.x` patch releases):** the two protocol constants and `STORE_FORMAT_VERSION` are unchanged since `v<PREV>` (when the one-version window is open `PROTOCOL_VERSION_MIN_COMPATIBLE == PROTOCOL_VERSION_CURRENT - 1`; when closed `PROTOCOL_VERSION_MIN_COMPATIBLE == PROTOCOL_VERSION_CURRENT`, [design 0083](../design/0083-one-version-window.md) D1). Note `"no wire change"` in the pull request and changelog.
- **Wire change:** the wire-change pull request itself bumps `PROTOCOL_VERSION_CURRENT` and states `"wire change"` in its description; if the change cannot be carried across the window, that pull request sets `PROTOCOL_VERSION_MIN_COMPATIBLE = PROTOCOL_VERSION_CURRENT`, states `"wire change, window closed"`, and deletes `crates/core/tests/data/wire_layout_previous.txt` (which exists only while `MIN < CURRENT`, with its `# protocol_version` header equal to `MIN`, [design 0083](../design/0083-one-version-window.md) D1 and D7). A release pull request never edits those constants. Cut the release within 48 hours of the wire-change merge, or before the next wire change merges, whichever comes first ([design 0076](../design/0076-protocol-compatibility-and-release-train.md) D1.2).

On the release author's workstation:

```bash
git fetch origin --tags
git diff v<PREV> origin/master -- crates/core/src/protocol.rs crates/master/src/store.rs | grep -E '^[-+].*(PROTOCOL_VERSION_(CURRENT|MIN_COMPATIBLE)|STORE_FORMAT_VERSION)'
grep -n -E "PROTOCOL_VERSION_(CURRENT|MIN_COMPATIBLE)" crates/core/src/protocol.rs
grep -n "STORE_FORMAT_VERSION" crates/master/src/store.rs
```

### 2. Get the owner's go-ahead and prepare the release pull request

1. Ask the owner for explicit approval to cut `<X.Y.Z>` and record the date and exact quote (`"<OWNER_APPROVAL>"`) for the pull request description (matching pull requests `321` and `339`).
2. Create `<AUTHOR>/release-<X.Y.Z>` from `origin/master` and update the release files:
   - [Cargo.toml](../../Cargo.toml): set `[workspace.package] version = "<X.Y.Z>"`. Workspace crates inherit `version.workspace = true`; keep `version = "0.0.0"` in `fuzz/Cargo.toml`.
   - [Cargo.lock](../../Cargo.lock) and [fuzz/Cargo.lock](../../fuzz/Cargo.lock): refresh the OxideSwarm workspace package version entries and confirm `cargo check --workspace --all-targets --locked` passes.
   - [CHANGELOG.md](../../CHANGELOG.md): move the items under `## [Unreleased]` into `## [<X.Y.Z>] - <YYYY-MM-DD>`, write a summary paragraph naming protocol and store compatibility, and add `### Upgrading from <PREV>`. Leave an empty `## [Unreleased]` section at the top.
   - [README.md](../../README.md): update `the latest is v<X.Y.Z>` and the archive filenames in `Install`.
   - [docs/reference.md](../reference.md): update `the latest is v<X.Y.Z>` and the archive filenames in `Install`, and `OxideSwarm <X.Y.Z> covers the user story above` in `Status and limits`.
   - [DEPLOYMENT.md](../../DEPLOYMENT.md): update the protocol release range line in section `3.5`.
   - [ROADMAP.md](../ROADMAP.md): update the dated `Releases (<YYYY-MM-DD>):` bullet under `Where the project stands` and add a row to the `Releases` table (no status-log entry).
   - [BACKLOG.md](../BACKLOG.md): append one dated line `Release <X.Y.Z> prepared (<wire and contents summary>)` to `## Status log`.

On the release author's workstation:

```bash
git switch -c <AUTHOR>/release-<X.Y.Z> origin/master
# After editing Cargo.toml, refresh the workspace entries in Cargo.lock and fuzz/Cargo.lock:
cargo metadata --format-version 1 > /dev/null
(cd fuzz && cargo metadata --format-version 1 > /dev/null)
cargo check --workspace --all-targets --locked
```

3. Commit the release preparation commit on `<AUTHOR>/release-<X.Y.Z>`, push it, and open the pull request quoting the owner's approval, protocol/store compatibility (`mixes freely with <PREV>`), changed files, contents, and post-merge tag/smoke steps (modeled on pull requests `321` and `339`). Because `.github/workflows/release.yml` triggers on pull requests touching `Cargo.toml` and `Cargo.lock` (building all eight targets, writing `SHA256SUMS`, and running `actions/attest` without creating a release), wait for both the `CI` and `Release` checks to pass. Run `gh workflow run release.yml --ref <AUTHOR>/release-<X.Y.Z> -f mode=attest-only` only when testing changes to `.github/workflows/release.yml` itself.

On the release author's workstation:

```bash
git add Cargo.toml Cargo.lock fuzz/Cargo.lock CHANGELOG.md README.md docs/reference.md DEPLOYMENT.md docs/ROADMAP.md docs/BACKLOG.md
git commit -m "release: prepare <X.Y.Z> (no wire change)"
git push -u origin <AUTHOR>/release-<X.Y.Z>
gh pr create --title "release: prepare <X.Y.Z> (no wire change)" --body "Release preparation for <X.Y.Z>, approved by the owner in chat on <YYYY-MM-DD> (\"<OWNER_APPROVAL>\"). No wire change: protocol <P>, store format <S>; it mixes freely with <PREV>.\n\n- Versions: Cargo.toml, Cargo.lock, fuzz/Cargo.lock\n- CHANGELOG.md, README.md, docs/reference.md, DEPLOYMENT.md, docs/ROADMAP.md, docs/BACKLOG.md\n\nAfter merge: tag v<X.Y.Z> on the merge commit, then run the Mac and Windows release checks."
```

### 3. Merge and push the `v<X.Y.Z>` tag to run `.github/workflows/release.yml`

**Owner-only step today.** Merging into protected `master` and pushing a `v*` tag require repository maintainer/admin rights held by the owner until step 3 of `LV-15` grants a second maintainer those rights. Even once a second maintainer holds merge and tag rights, tagging still waits for the owner's explicit yes until the owner changes that rule. Publishing the GitHub release itself is done automatically by the `publish` job's `GITHUB_TOKEN` (`contents: write`).

After CI and Release checks pass and reviews are green, merge the pull request to `master`, read the exact merge commit SHA, create an annotated tag `v<X.Y.Z>` with `-m "OxideSwarm <X.Y.Z>"`, and push the tag. Pushing `v<X.Y.Z>` triggers [.github/workflows/release.yml](../../.github/workflows/release.yml):

1. `build` compiles release binaries (`--locked`, with and without `--features gpu-wgpu`) and packages `oxideswarm`, `rusty-grid`, `ox-mode`, licences, and scripts for eight targets (`x86_64-unknown-linux-gnu`, `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-pc-windows-msvc`, each standard and `-gpu`).
2. `checksums` downloads all eight archives, writes `SHA256SUMS`, verifies `sha256sum -c SHA256SUMS`, and runs `actions/attest` to record one signed SLSA build provenance attestation covering all eight archives.
3. `publish` downloads all nine assets (`8` archives plus `SHA256SUMS`) and runs `gh release create` with `--verify-tag`.

On the owner's workstation:

```bash
gh pr merge <PR_NUMBER> --merge
git fetch origin
MERGE_SHA="$(gh pr view <PR_NUMBER> --json mergeCommit --jq .mergeCommit.oid)"
git tag -a v<X.Y.Z> "$MERGE_SHA" -m "OxideSwarm <X.Y.Z>"
git push origin "v<X.Y.Z>"
```

Watch the workflow run until every job succeeds and `gh release view v<X.Y.Z>` lists all nine assets.

On the release author's workstation:

```bash
gh run list --workflow release.yml --limit 5
gh release view v<X.Y.Z>
```

### 4. Verify all nine release assets and build provenance

Download `SHA256SUMS` and every archive into a fresh directory, verify every checksum line, and verify build provenance with `gh attestation verify` (`LV-5` landed after `v0.4.3`, so attestation verification applies to every release after `v0.4.3`).

On a release verifier's workstation (macOS or Linux):

```bash
mkdir -p "<VERIFY_DIR>" && cd "<VERIFY_DIR>"
gh release download v<X.Y.Z> --repo DuongNAD/OxideSwarm
shasum -a 256 -c SHA256SUMS          # Linux: sha256sum -c SHA256SUMS
for archive in oxideswarm-*.tar.gz oxideswarm-*.zip; do
  gh attestation verify "$archive" --repo DuongNAD/OxideSwarm
done
```

### 5. Run the macOS archive smoke check

Run the smoke script inside a subshell `( ... )` with a private directory, isolated `HOME`, `USERPROFILE`, `OXIDESWARM_HOME`, and `TMPDIR`, ephemeral ports (`127.0.0.1:0`, `--discovery-port 0`, `--dashboard-port 0`), and separate worker `--sandbox-base-dir` directories so nothing leaks into your outer shell. Stop every process by its PID, never by process name.

On the macOS (`<ARCH>`) test machine:

```bash
(
  set -euo pipefail
  TEST_ROOT="$(mktemp -d)"
  chmod 700 "$TEST_ROOT"
  mkdir -p "$TEST_ROOT/home" "$TEST_ROOT/blobs" "$TEST_ROOT/sb1" "$TEST_ROOT/sb2" "$TEST_ROOT/verify"
  cd "$TEST_ROOT"

  gh release download v<X.Y.Z> --repo DuongNAD/OxideSwarm -p "oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin.tar.gz" -p SHA256SUMS
  shasum -a 256 -c SHA256SUMS --ignore-missing
  gh attestation verify "oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin.tar.gz" --repo DuongNAD/OxideSwarm   # releases after v0.4.3
  tar -xzf "oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin.tar.gz"
  xattr -dr com.apple.quarantine "oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin"

  BIN="$TEST_ROOT/oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin/oxideswarm"
  "$BIN" --version

  export HOME="$TEST_ROOT/home" USERPROFILE="$TEST_ROOT/home" OXIDESWARM_HOME="$TEST_ROOT/home/.oxideswarm" TMPDIR="$TEST_ROOT"
  "$BIN" master --listen 127.0.0.1:0 --port-file "$TEST_ROOT/master.port" --discovery-port 0 --dashboard-port 0 --blob-dir "$TEST_ROOT/blobs" > "$TEST_ROOT/master.log" 2>&1 &
  MASTER_PID=$!

  for _ in $(seq 1 50); do [ -s "$TEST_ROOT/master.port" ] && break; sleep 0.2; done
  MASTER_ADDR="127.0.0.1:$(cat "$TEST_ROOT/master.port")"

  "$BIN" worker --master "$MASTER_ADDR" --name smoke-w1 --no-gpu --no-portal --sandbox-base-dir "$TEST_ROOT/sb1" > "$TEST_ROOT/w1.log" 2>&1 &
  W1_PID=$!
  "$BIN" worker --master "$MASTER_ADDR" --name smoke-w2 --no-gpu --no-portal --sandbox-base-dir "$TEST_ROOT/sb2" > "$TEST_ROOT/w2.log" 2>&1 &
  W2_PID=$!

  sleep 2
  "$BIN" submit --master "$MASTER_ADDR" --wait -- echo hi
  "$BIN" submit --master "$MASTER_ADDR" --verify 1 --output det.txt --output-dir "$TEST_ROOT/verify" --type shell --command "printf 'deterministic\n' > det.txt"
  "$BIN" doctor --master "$MASTER_ADDR" --expect-workers 2 --skip-gpu

  kill "$W2_PID" "$W1_PID" "$MASTER_PID"
  wait "$W2_PID" "$W1_PID" "$MASTER_PID" 2>/dev/null || true
)
```

### 6. Run the Windows archive smoke check

Repeat the same isolated loopback check on Windows using ephemeral ports and stopping the processes by their PIDs.

On the Windows (`x86_64`) test machine:

```powershell
$TestRoot = Join-Path $env:LOCALAPPDATA "OxideSwarm\ox-rel-smoke-$([guid]::NewGuid().ToString('N').Substring(0,8))"
New-Item -ItemType Directory -Force -Path "$TestRoot\home","$TestRoot\blobs","$TestRoot\sb1","$TestRoot\sb2","$TestRoot\verify" | Out-Null
Set-Location $TestRoot

gh release download v<X.Y.Z> --repo DuongNAD/OxideSwarm -p "oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc.zip" -p SHA256SUMS
$expected = (Select-String -Path SHA256SUMS -Pattern "oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc\.zip$").Line.Split()[0].ToUpper()
$actual = (Get-FileHash ".\oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc.zip" -Algorithm SHA256).Hash.ToUpper()
if ($actual -ne $expected) { throw "SHA256 mismatch: $actual != $expected" }
gh attestation verify ".\oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc.zip" --repo DuongNAD/OxideSwarm   # releases after v0.4.3

Expand-Archive ".\oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc.zip" -DestinationPath "$TestRoot\unpacked"
$Bin = "$TestRoot\unpacked\oxideswarm-<X.Y.Z>-x86_64-pc-windows-msvc\oxideswarm.exe"
& $Bin --version

$env:USERPROFILE = "$TestRoot\home"
$env:HOME = "$TestRoot\home"
$env:OXIDESWARM_HOME = "$TestRoot\home\.oxideswarm"

$m = Start-Process -FilePath $Bin -ArgumentList "master","--listen","127.0.0.1:0","--port-file","$TestRoot\master.port","--discovery-port","0","--dashboard-port","0","--blob-dir","$TestRoot\blobs" -PassThru -WindowStyle Hidden
for ($i = 0; $i -lt 50 -and -not (Test-Path "$TestRoot\master.port"); $i++) { Start-Sleep -Milliseconds 200 }
$MasterAddr = "127.0.0.1:$((Get-Content "$TestRoot\master.port").Trim())"

$w1 = Start-Process -FilePath $Bin -ArgumentList "worker","--master",$MasterAddr,"--name","smoke-w1","--no-gpu","--no-portal","--sandbox-base-dir","$TestRoot\sb1" -PassThru -WindowStyle Hidden
$w2 = Start-Process -FilePath $Bin -ArgumentList "worker","--master",$MasterAddr,"--name","smoke-w2","--no-gpu","--no-portal","--sandbox-base-dir","$TestRoot\sb2" -PassThru -WindowStyle Hidden

Start-Sleep -Seconds 2
& $Bin submit --master $MasterAddr --wait -- cmd /c echo hi
& $Bin submit --master $MasterAddr --verify 1 --output det.txt --output-dir "$TestRoot\verify" -- cmd /c "echo deterministic>det.txt"
& $Bin doctor --master $MasterAddr --expect-workers 2 --skip-gpu

Stop-Process -Id $w2.Id, $w1.Id, $m.Id
```

### 7. Check post-release changelog lines

After the release merges, check [CHANGELOG.md](../../CHANGELOG.md) on `origin/master` so any newly merged pull requests placed their entries under `## [Unreleased]` rather than under the released `## [<X.Y.Z>]` section ([CLAUDE.md](../../CLAUDE.md)).

On the release author's workstation:

```bash
git fetch origin
git show origin/master:CHANGELOG.md | sed -n '1,30p'
```

## Verify

On each test machine, confirm the release check output includes these exact forms:

```text
oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin.tar.gz: OK
✓ Verification succeeded!
rusty-grid <X.Y.Z>
...
Stdout:
hi
...
verified: 1 file(s) identical on 2 workers (smoke-w2, smoke-w1, all <OS>/<ARCH>)
...
Summary: <N> passed, 0 warnings, 0 failed, <N> skipped
```

Record the macOS and Windows check results with exact output lines before closing the release. When a second maintainer completes a release from this runbook alone (`LV-15` step 4, proof run V3), record its date, any unclear step, and the verification outcome in the status log of [design 0058](../design/0058-lasting-value.md).

## Accounts and secrets

Skeleton inventory for `LV-15` step 2 ([design 0058](../design/0058-lasting-value.md) D5). This table names only the resource, required permission or credential type, and holder; never write a secret value or token in this guide or in the repository.

| Dependency / Resource | Purpose | Credential / Workflow Permission Scope | Who holds it |
|---|---|---|---|
| GitHub repository `DuongNAD/OxideSwarm` | Protected `master` merge, `v*` tag push, GitHub Releases, branch protection | Maintainer / admin repository role (`merge`, `push` tags `v*`, manage releases) | owner (to confirm) |
| GitHub Actions: [.github/workflows/ci.yml](../../.github/workflows/ci.yml) | Workspace build, clippy, fmt, `cargo deny`, and test matrix | Automatic `GITHUB_TOKEN` with `contents: read` only (no static secrets) | owner (to confirm) |
| GitHub Actions: [.github/workflows/release.yml](../../.github/workflows/release.yml) | Build 8 release targets, write `SHA256SUMS`, attest SLSA provenance, create release | Automatic `GITHUB_TOKEN` / GitHub OIDC (`contents: read` top-level; `checksums`: `id-token: write`, `attestations: write`; `publish`: `contents: write`; no static signing key) | owner (to confirm) |
| GitHub Actions: [.github/workflows/scorecard.yml](../../.github/workflows/scorecard.yml) | Weekly and `master`-push OpenSSF Scorecard scan | Automatic `GITHUB_TOKEN` (`contents: read`, `security-events: write`, `id-token: write`) | owner (to confirm) |
| GitGuardian (`GitGuardian Security Checks`) | Secret scanning status check on every pull request and push | GitHub App installed on `DuongNAD/OxideSwarm` | owner (to confirm) |
| `crates.io` | Not published today (`publish = false` across workspace crates in `Cargo.toml`) | None required today; reserve crate ownership policy before publishing | owner (to confirm) |
| Domains | Public endpoints (`github.com/DuongNAD/OxideSwarm`, default `n0` iroh relays) | None | none registered today (to confirm) |
| Agent coordination hub (`OxideSwarm_Agent_Hub`) | Multi-machine review and release smoke-test logs (never holds secrets, IPs, or tickets) | Shared Google Drive folder access | owner (to confirm) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `cargo check --workspace --all-targets --locked` fails after editing `Cargo.toml` | `Cargo.lock` or `fuzz/Cargo.lock` still records the previous OxideSwarm crate version | Run `cargo metadata --format-version 1` at the root and in `fuzz/` so both lock files match `Cargo.toml` |
| macOS Gatekeeper blocks the unpacked binary (`cannot be opened because the developer cannot be verified`) | Release binaries are unsigned; browser downloads carry `com.apple.quarantine` | Run `xattr -dr com.apple.quarantine oxideswarm-<X.Y.Z>-<ARCH>-apple-darwin` |
| `gh attestation verify` fails with no attestation found | Archive is from `v0.4.3` or earlier (before `LV-5` added `actions/attest`), or archive bytes differ from the CI build | Check `SHA256SUMS`; use attestation verification on releases after `v0.4.3` or a `release.yml` `attest-only` run |
| `submit --verify 1` fails with fewer than 1 other worker | Only one worker connected to the smoke master, or both workers shared the same worker ID | Start two workers with distinct `--name` values and separate `--sandbox-base-dir` paths |
| Second worker fails to bind port `8080` | Standby portal collides with the first worker or another process on `:8080` | Pass `--no-portal` to both smoke workers and `--dashboard-port 0` to the master |
| Windows smoke worker exits with `Task sandboxes cannot use '...' safely` | `%TEMP%` (or `AppData` on multi-account hosts) carries write ACEs for another account or leftover SID (`ensure_private_dir`) | Keep `$TestRoot` under `%LOCALAPPDATA%` as above, or use a private folder directly under the drive root (for example `C:\OxideSwarm\rel-smoke`) when `AppData` itself is shared |
