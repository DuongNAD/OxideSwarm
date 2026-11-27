# Install OxideSwarm binaries (oxideswarm.exe, rusty-grid.exe, ox-mode.exe) from a pinned release.
# Design 0059 D6 (ADOPT-3): current-user install only; no elevation, no service, no daemon start,
# no secret handling, and no edits to %USERPROFILE%\.oxideswarm.
# Pure ASCII for Windows PowerShell 5.1 compatibility.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$DefaultVersion = '0.4.6'
$Repo = 'DuongNAD/OxideSwarm'
$SignerWorkflow = 'DuongNAD/OxideSwarm/.github/workflows/release.yml'
$DefaultDownloadBase = 'https://github.com/DuongNAD/OxideSwarm/releases/download'

function Write-Usage {
    @'
Usage: install.ps1 [--version <VERSION>] [--dir <DIR>] [--gpu] [--dry-run] [-h|--help]
       install.ps1 [-Version <VERSION>] [-Dir <DIR>] [-Gpu] [-DryRun] [-Help]

Install oxideswarm.exe, rusty-grid.exe, and ox-mode.exe for the current user
after verifying SHA256SUMS (and GitHub build provenance attestation when `gh` is installed and authenticated).

Options:
  --version, -Version <VERSION>  Release version to install (default: 0.4.6)
  --dir, -Dir <DIR>              Destination directory (default: %LOCALAPPDATA%\OxideSwarm\bin)
  --gpu, -Gpu                    Install the -gpu build (with wgpu support)
  --dry-run, -DryRun             Print resolved target and URLs without downloading or writing files
  -h, --help, -Help              Show this help message and exit

Environment:
  OXIDESWARM_DOWNLOAD_BASE       Base URL for release downloads
                                 (default: https://github.com/DuongNAD/OxideSwarm/releases/download)
'@ | Write-Output
}

function Fail-Install([string]$Message, [int]$Code = 1) {
    [Console]::Error.WriteLine(('error: ' + $Message))
    exit $Code
}

$Version = $DefaultVersion
$TargetDir = ''
$DryRun = $false
$Gpu = $false

$i = 0
while ($i -lt $args.Count) {
    $arg = [string]$args[$i]
    if ($arg -eq '--version' -or $arg -eq '-Version') {
        if ($i + 1 -ge $args.Count) { Fail-Install "$arg requires a value" 2 }
        $Version = [string]$args[$i + 1]
        $i += 2
    } elseif ($arg -match '^--version=(.*)$') {
        $Version = $Matches[1]
        if ([string]::IsNullOrWhiteSpace($Version)) { Fail-Install '--version requires a value' 2 }
        $i += 1
    } elseif ($arg -eq '--dir' -or $arg -eq '-Dir') {
        if ($i + 1 -ge $args.Count) { Fail-Install "$arg requires a value" 2 }
        $TargetDir = [string]$args[$i + 1]
        $i += 2
    } elseif ($arg -match '^--dir=(.*)$') {
        $TargetDir = $Matches[1]
        if ([string]::IsNullOrWhiteSpace($TargetDir)) { Fail-Install '--dir requires a value' 2 }
        $i += 1
    } elseif ($arg -eq '--dry-run' -or $arg -eq '-DryRun') {
        $DryRun = $true
        $i += 1
    } elseif ($arg -eq '--gpu' -or $arg -eq '-Gpu') {
        $Gpu = $true
        $i += 1
    } elseif ($arg -eq '-h' -or $arg -eq '--help' -or $arg -eq '-Help') {
        Write-Usage
        exit 0
    } else {
        [Console]::Error.WriteLine("error: unknown argument: $arg")
        Write-Usage | ForEach-Object { [Console]::Error.WriteLine($_) }
        exit 2
    }
}

if ($Version.StartsWith('v') -or $Version.StartsWith('V')) {
    $Version = $Version.Substring(1)
}
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$' -or $Version.Contains('..')) {
    Fail-Install ("invalid version: " + $Version) 2
}

if ([string]::IsNullOrWhiteSpace($TargetDir)) {
    $localApp = $env:LOCALAPPDATA
    if ([string]::IsNullOrWhiteSpace($localApp) -and -not [string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
        $localApp = Join-Path $env:USERPROFILE 'AppData\Local'
    }
    if ([string]::IsNullOrWhiteSpace($localApp)) {
        Fail-Install 'LOCALAPPDATA and USERPROFILE are not set; pass --dir <DIR>' 1
    }
    $TargetDir = Join-Path $localApp 'OxideSwarm\bin'
}

$rawArch = $env:PROCESSOR_ARCHITEW6432
if ([string]::IsNullOrWhiteSpace($rawArch)) {
    $rawArch = $env:PROCESSOR_ARCHITECTURE
}
if ([string]::IsNullOrWhiteSpace($rawArch)) {
    $rawArch = 'unknown'
}

switch ($rawArch.ToUpperInvariant()) {
    'AMD64' { $Target = 'x86_64-pc-windows-msvc' }
    default {
        Fail-Install ("unsupported platform (Windows / $rawArch): no release archive exists for Windows architecture '$rawArch' (supported: AMD64 -> x86_64-pc-windows-msvc)") 1
    }
}

$DownloadBase = $env:OXIDESWARM_DOWNLOAD_BASE
if ([string]::IsNullOrWhiteSpace($DownloadBase)) {
    $DownloadBase = $DefaultDownloadBase
}
$DownloadBase = $DownloadBase.TrimEnd('/')
if ([string]::IsNullOrWhiteSpace($DownloadBase)) {
    Fail-Install 'OXIDESWARM_DOWNLOAD_BASE must not be empty' 2
}

$ArchiveStem = if ($Gpu) { "oxideswarm-$Version-$Target-gpu" } else { "oxideswarm-$Version-$Target" }
$ArchiveName = "$ArchiveStem.zip"
$ArchiveUrl = "$DownloadBase/v$Version/$ArchiveName"
$SumsUrl = "$DownloadBase/v$Version/SHA256SUMS"

function Test-GhAuthenticated($Cmd) {
    if (-not $Cmd) { return $false }
    if (-not [string]::IsNullOrWhiteSpace($env:GH_TOKEN) -or -not [string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) {
        return $true
    }
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Cmd.Source auth token 2>$null | Out-Null
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $prevEap
    }
    return ($code -eq 0)
}

$GhCmd = Get-Command gh -ErrorAction SilentlyContinue

if ($DryRun) {
    if (-not $GhCmd) {
        $AttestPlan = 'skip (gh CLI not found; SHA256SUMS only)'
    } elseif (Test-GhAuthenticated $GhCmd) {
        $AttestPlan = 'verify with gh attestation verify'
    } else {
        $AttestPlan = 'skip (gh CLI is not authenticated; SHA256SUMS only)'
    }
    Write-Output 'Dry run: no files will be downloaded or written.'
    Write-Output "  version:     $Version"
    Write-Output "  platform:    Windows / $rawArch -> $Target"
    Write-Output "  archive:     $ArchiveUrl"
    Write-Output "  checksums:   $SumsUrl"
    Write-Output "  attestation: $AttestPlan"
    Write-Output "  destination: $TargetDir"
    exit 0
}

$TempRoot = Join-Path ([IO.Path]::GetTempPath()) ('oxideswarm-install-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($TempRoot)
try {
    $ArchivePath = Join-Path $TempRoot $ArchiveName
    $SumsPath = Join-Path $TempRoot 'SHA256SUMS'
    $UnpackDir = Join-Path $TempRoot 'unpack'

    $ProgressPreference = 'SilentlyContinue'
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    } catch {}

    Write-Output "Downloading $ArchiveUrl..."
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $ArchiveUrl -OutFile $ArchivePath
    } catch {
        Fail-Install ("failed to download archive ${ArchiveUrl}: " + $_.Exception.Message) 1
    }

    Write-Output "Downloading $SumsUrl..."
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $SumsUrl -OutFile $SumsPath
    } catch {
        Fail-Install ("failed to download checksums ${SumsUrl}: " + $_.Exception.Message) 1
    }

    $ExpectedSha = $null
    foreach ($rawLine in (Get-Content -LiteralPath $SumsPath -Encoding UTF8)) {
        $line = $rawLine.TrimEnd("`r", "`n")
        if ($line -match '^([0-9A-Fa-f]{64})[ \t]+(\*?)(\./)?(.+)$') {
            $candidateName = $Matches[4]
            if ($candidateName -ceq $ArchiveName) {
                $ExpectedSha = $Matches[1].ToLowerInvariant()
                break
            }
        }
    }
    if (-not $ExpectedSha) {
        Fail-Install "$ArchiveName entry not found in SHA256SUMS" 1
    }

    $ActualSha = (Get-FileHash -LiteralPath $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($ActualSha -cne $ExpectedSha) {
        [Console]::Error.WriteLine("error: SHA-256 checksum mismatch for $ArchiveName")
        [Console]::Error.WriteLine("  expected: $ExpectedSha")
        [Console]::Error.WriteLine("  actual:   $ActualSha")
        exit 1
    }
    Write-Output "Checksum verified ($ActualSha)."

    if ($GhCmd) {
        if (Test-GhAuthenticated $GhCmd) {
            Write-Output 'Verifying release provenance attestation with gh...'
            $prevEap = $ErrorActionPreference
            $ErrorActionPreference = 'Continue'
            & $GhCmd.Source attestation verify $ArchivePath --repo $Repo --signer-workflow $SignerWorkflow --source-ref ("refs/tags/v" + $Version)
            $ghExit = $LASTEXITCODE
            $ErrorActionPreference = $prevEap
            if ($ghExit -ne 0) {
                Fail-Install "provenance attestation verification failed for $ArchiveName" 1
            }
            Write-Output 'Provenance attestation verified.'
        } else {
            Write-Output "Warning: gh CLI is installed but not authenticated; skipping provenance attestation check (SHA-256 verified). Run 'gh auth login' to enable provenance verification."
        }
    } else {
        Write-Output 'Note: gh CLI not found; skipping provenance attestation check (SHA-256 verified).'
    }

    [void][IO.Directory]::CreateDirectory($UnpackDir)
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $UnpackFull = [IO.Path]::GetFullPath($UnpackDir)
    if (-not $UnpackFull.EndsWith([IO.Path]::DirectorySeparatorChar)) {
        $UnpackFull += [IO.Path]::DirectorySeparatorChar
    }
    $zip = [IO.Compression.ZipFile]::OpenRead($ArchivePath)
    try {
        foreach ($entry in $zip.Entries) {
            $rel = $entry.FullName -replace '/', [IO.Path]::DirectorySeparatorChar
            $destPath = [IO.Path]::GetFullPath((Join-Path $UnpackDir $rel))
            if (-not $destPath.StartsWith($UnpackFull, [StringComparison]::OrdinalIgnoreCase)) {
                Fail-Install ("refusing unsafe archive entry outside unpack dir: " + $entry.FullName) 1
            }
            if ([string]::IsNullOrEmpty($entry.Name)) {
                [void][IO.Directory]::CreateDirectory($destPath)
            } else {
                $parentDir = [IO.Path]::GetDirectoryName($destPath)
                if (-not [string]::IsNullOrEmpty($parentDir)) {
                    [void][IO.Directory]::CreateDirectory($parentDir)
                }
                [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $destPath, $true)
            }
        }
    } finally {
        $zip.Dispose()
    }

    $CandidateSub = Join-Path $UnpackDir $ArchiveStem
    if (Test-Path -LiteralPath $CandidateSub) {
        $subItem = Get-Item -LiteralPath $CandidateSub -Force
        if (($subItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or -not $subItem.PSIsContainer) {
            Fail-Install "archive entry '$ArchiveStem' is not a regular directory" 1
        }
        $SrcDir = $CandidateSub
    } else {
        $SrcDir = $UnpackDir
    }

    $Bins = @('oxideswarm.exe', 'rusty-grid.exe', 'ox-mode.exe')
    foreach ($b in $Bins) {
        $srcBin = Join-Path $SrcDir $b
        if (-not (Test-Path -LiteralPath $srcBin -PathType Leaf)) {
            Fail-Install "expected binary '$b' missing in archive $ArchiveName" 1
        }
        $item = Get-Item -LiteralPath $srcBin -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            Fail-Install "refusing symlink or reparse point binary '$b' in archive $ArchiveName" 1
        }
    }

    if (Test-Path -LiteralPath $TargetDir) {
        $dstItem = Get-Item -LiteralPath $TargetDir -Force
        if (($dstItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or -not $dstItem.PSIsContainer) {
            Fail-Install "destination $TargetDir exists and is not a regular directory" 1
        }
    } else {
        [void][IO.Directory]::CreateDirectory($TargetDir)
    }

    foreach ($b in $Bins) {
        $srcBin = Join-Path $SrcDir $b
        $dstBin = Join-Path $TargetDir $b
        $tmpBin = Join-Path $TargetDir (".$b.tmp.$PID." + [guid]::NewGuid().ToString('N').Substring(0, 8))
        try {
            [IO.File]::Copy($srcBin, $tmpBin, $true)
            Move-Item -LiteralPath $tmpBin -Destination $dstBin -Force
        } finally {
            if (Test-Path -LiteralPath $tmpBin) {
                Remove-Item -LiteralPath $tmpBin -Force -ErrorAction SilentlyContinue
            }
        }
    }

    Write-Output ''
    Write-Output "Installed OxideSwarm $Version ($Target) into $TargetDir"
    $rawPath = if ($env:PATH) { $env:PATH } else { '' }
    $pathParts = @($rawPath -split ';' | ForEach-Object { $_.TrimEnd('\') })
    if ($pathParts -notcontains $TargetDir.TrimEnd('\')) {
        Write-Output 'Add it to PATH for this session:'
        Write-Output "  `$env:PATH = `"$TargetDir;`$env:PATH`""
    }
    Write-Output ''
    Write-Output 'Quick start (three terminals, loopback):'
    Write-Output '  oxideswarm master'
    Write-Output '  oxideswarm worker --workspaces'
    Write-Output '  oxideswarm submit --wait -- hostname'
} finally {
    if (Test-Path -LiteralPath $TempRoot) {
        Remove-Item -LiteralPath $TempRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
