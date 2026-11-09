# OxideSwarm USB installer for Windows (kit format v0, design 0053 prototype).
#
# Installs the worker for the current user from this USB stick (no administrator rights), joins the
# cluster with the kit's join token, starts the worker hidden, and starts it again at every logon
# through an HKCU Run entry. The user's own %USERPROFILE%\.oxideswarm is never read or written:
# the installed worker keeps its state in <install folder>\home.
#
#   powershell -ExecutionPolicy Bypass -File install.ps1 [-Name NAME] [-InstallRoot DIR] [-NoAutostart] [-NoWorkspaces]
param(
    [string]$Name = "",
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA "OxideSwarm"),
    [switch]$NoAutostart,
    [switch]$NoWorkspaces
)
$ErrorActionPreference = 'Stop'
$Kit = $PSScriptRoot
function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

$Target = 'x86_64-pc-windows-msvc'
if (-not [Environment]::Is64BitOperatingSystem) { Fail "this kit needs 64-bit Windows" }
$TokenFile = Join-Path $Kit 'join.token'
# A kit made with make_kit.sh --no-autostart is meant for a trial on someone else's machine.
if (Test-Path (Join-Path $Kit 'no-autostart')) { $NoAutostart = $true }
$Sums = Join-Path $Kit 'archives\SHA256SUMS'
if (-not (Test-Path $TokenFile)) { Fail "join.token is missing from the kit" }
if (-not (Test-Path $Sums)) { Fail "archives\SHA256SUMS is missing from the kit" }
$Archive = Get-ChildItem (Join-Path $Kit 'archives') -Filter "oxideswarm-*-$Target.zip" | Select-Object -First 1
if (-not $Archive) { Fail "no archive for $Target in the kit" }

Write-Host "OxideSwarm USB installer"
Write-Host "  folder:  $InstallRoot"

# 1. Check the archive against the kit's SHA256SUMS (catches a damaged copy, not a swapped stick).
$expected = $null
foreach ($line in Get-Content $Sums) {
    $parts = $line -split '\s+', 2
    if ($parts.Count -eq 2 -and $parts[1].Trim() -eq $Archive.Name) { $expected = $parts[0].ToLower() }
}
if (-not $expected) { Fail "$($Archive.Name) is not listed in SHA256SUMS" }
$actual = (Get-FileHash $Archive.FullName -Algorithm SHA256).Hash.ToLower()
if ($expected -ne $actual) { Fail "checksum mismatch for $($Archive.Name): the copy on the stick is damaged" }
Write-Host "  archive: $($Archive.Name) (checksum OK)"

# 2. Unpack the binaries.
$Bin = Join-Path $InstallRoot 'bin'
$State = Join-Path $InstallRoot 'home'
$RunDir = Join-Path $InstallRoot 'run'
$LogDir = Join-Path $InstallRoot 'logs'
foreach ($d in @($Bin, $State, $RunDir, $LogDir)) { New-Item -ItemType Directory -Force -Path $d | Out-Null }
$Tmp = Join-Path ([IO.Path]::GetTempPath()) ("oxideswarm-usb-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Expand-Archive -Path $Archive.FullName -DestinationPath $Tmp -Force
    $src = Get-ChildItem $Tmp -Recurse -Filter 'oxideswarm.exe' | Select-Object -First 1
    if (-not $src) { Fail "the archive holds no oxideswarm.exe" }

    # 3. Stop a worker that an earlier run of this installer started (by its PID, never by name).
    $pidFile = Join-Path $RunDir 'worker.pid'
    if (Test-Path $pidFile) {
        $old = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($old) {
            $p = Get-Process -Id ([int]$old) -ErrorAction SilentlyContinue
            if ($p -and $p.Path -like "$Bin*") {
                Write-Host "  stopping the worker started earlier (PID $old)"
                Stop-Process -Id ([int]$old) -Force
                Start-Sleep -Seconds 2
            }
        }
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
    # A worker started at logon by an earlier install has no PID file entry: match it by the exact
    # path of this install's binary, never by process name.
    $installedExe = Join-Path $Bin 'oxideswarm.exe'
    Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq $installedExe } | ForEach-Object {
        Write-Host "  stopping the worker started earlier (PID $($_.ProcessId))"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }
    foreach ($b in @('oxideswarm.exe', 'rusty-grid.exe', 'ox-mode.exe')) {
        $f = Join-Path $src.DirectoryName $b
        if (Test-Path $f) { Copy-Item $f (Join-Path $Bin $b) -Force }
    }
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
$Exe = Join-Path $Bin 'oxideswarm.exe'
$version = & $Exe --version
if ($LASTEXITCODE -ne 0) { Fail "the unpacked binary does not run on this machine" }
Write-Host "  binary:  $version"

# Every command below keeps its state in $State and runs from $InstallRoot.
$env:OXIDESWARM_HOME = $State
Set-Location $InstallRoot

# 4. Join: the token goes in on standard input only.
Write-Host ""
Write-Host "Joining the cluster ..."
Get-Content -Raw $TokenFile | & $Exe join --no-doctor
if ($LASTEXITCODE -ne 0) { Fail "join failed (see the message above); nothing was started" }

# The worker prefers a P2P ticket that join saved in [worker] p2p_ticket. On networks where the
# machines cannot reach each other directly (school or office Wi-Fi with client isolation), the
# client checks below must use that ticket too. A ticket is an address, not a credential.
$ticket = $null
$cfg = Join-Path $State 'rusty-grid.toml'
if (Test-Path $cfg) {
    $inWorker = $false
    foreach ($line in Get-Content $cfg) {
        if ($line -match '^\[') { $inWorker = ($line -match '^\[worker\]\s*$') }
        elseif ($inWorker -and $line -match '^\s*p2p_ticket\s*=\s*(.+?)\s*$') {
            $v = $Matches[1]
            if (($v.StartsWith("'") -and $v.EndsWith("'")) -or ($v.StartsWith('"') -and $v.EndsWith('"'))) { $v = $v.Substring(1, $v.Length - 2) }
            $ticket = $v
            break
        }
    }
}
$checkArgs = @()
if ($ticket) { $checkArgs = @('--master', $ticket); Write-Host "  link:    P2P ticket (the worker and the checks connect through iroh)" }

# 5. Worker options.
if (-not $Name) { $Name = ($env:COMPUTERNAME).ToLower() -replace '[^a-z0-9_-]', '-' }
# Use at most half of the CPU cores, so the machine stays responsive for its owner.
$slots = [Math]::Max(1, [int][Math]::Floor([Environment]::ProcessorCount / 2))
$workerArgs = "worker --name $Name --no-portal --max-concurrency $slots"
if (-not $NoWorkspaces -and (Get-Command cargo -ErrorAction SilentlyContinue)) { $workerArgs += " --workspaces" }
$log = Join-Path $LogDir 'worker.log'

# A small launcher that starts the worker hidden with this install's state folder. The HKCU Run
# entry uses it at every logon.
$vbs = Join-Path $InstallRoot 'start-worker.vbs'
$cmdLine = 'cmd /c ""' + $Exe + '" ' + $workerArgs + ' >> "' + $log + '" 2>&1"'
@(
    'Set sh = CreateObject("WScript.Shell")',
    ('sh.Environment("Process")("OXIDESWARM_HOME") = "' + $State + '"'),
    ('sh.CurrentDirectory = "' + $InstallRoot + '"'),
    ('sh.Run "' + $cmdLine.Replace('"', '""') + '", 0, False')
) | Set-Content -Path $vbs -Encoding ASCII

# 6. Start it now through the same hidden launcher, so that closing this window does not stop it.
Write-Host ""
Write-Host "Starting worker '$Name' ..."
Start-Process -FilePath 'wscript.exe' -ArgumentList ('"' + $vbs + '"') | Out-Null
$workerPid = $null
for ($i = 0; $i -lt 10 -and -not $workerPid; $i++) {
    Start-Sleep -Seconds 1
    $workerPid = Get-CimInstance Win32_Process |
        Where-Object { $_.ExecutablePath -eq $Exe -and $_.CommandLine -like '* worker *' } |
        Select-Object -First 1 -ExpandProperty ProcessId
}
if (-not $workerPid) { Fail "the worker did not start; see $log" }
$workerPid | Set-Content $pidFile
Start-Sleep -Seconds 3
if (-not (Get-Process -Id $workerPid -ErrorAction SilentlyContinue)) { Fail "the worker stopped right away; see $log" }

# 7. Wait until the master lists it.
$connected = $false
for ($i = 0; $i -lt 30; $i++) {
    # The JSON listing carries the real state; the plain listing also shows disconnected workers.
    $list = (& $Exe workers --json @checkArgs | Out-String) | ConvertFrom-Json
    if (@($list | Where-Object { $_.name -eq $Name -and $_.tags -contains 'state:connected' }).Count -gt 0) { $connected = $true; break }
    Start-Sleep -Seconds 1
}
if (-not $connected) { Fail "the worker runs but the master does not list it yet; see $log" }
Write-Host "  connected"

# 8. Start at logon (current user only, no administrator rights).
if (-not $NoAutostart) {
    $runKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
    Set-ItemProperty -Path $runKey -Name 'OxideSwarmUsbWorker' -Value ('wscript.exe "' + $vbs + '"')
    Write-Host "  autostart: HKCU Run entry OxideSwarmUsbWorker"
} else {
    Write-Host "  autostart: skipped (-NoAutostart)"
}

# 9. Check the cluster.
Write-Host ""
Write-Host "Checking the cluster (oxideswarm doctor) ..."
& $Exe doctor @checkArgs
$doctor = if ($LASTEXITCODE -eq 0) { "passed" } else { "reported a problem (see above)" }
Write-Host ""
Write-Host "Done. Worker '$Name' is part of the cluster; the doctor check $doctor."
Write-Host "Files: $InstallRoot   Log: $log"
Write-Host "To remove it, run 'Uninstall OxideSwarm.cmd' from the stick."
