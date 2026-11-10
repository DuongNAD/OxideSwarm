# OxideSwarm USB installer for Windows (kit format v0, design 0053 prototype).
#
# Installs the worker for the current user from this USB stick (no administrator rights), joins the
# cluster with the kit's join token, starts the worker hidden, and starts it again at every logon
# through an HKCU Run entry. The user's own %USERPROFILE%\.oxideswarm is never read or written:
# the installed worker keeps its state in <install folder>\home.
#
# The install folder is %LOCALAPPDATA%\OxideSwarm unless -InstallRoot names another one or the person
# at the keyboard picks another fixed NTFS or ReFS drive. Such a folder holds the task folders and
# the file cache too, and the installer limits it to this user, SYSTEM and Administrators.
#
# -Update (what "Update OxideSwarm.cmd" runs) updates an existing install in place: it keeps its
# folder, name, GPU choice, isolation, cluster membership and start at logon, asks nothing, does
# not join again, and only replaces the binaries and restarts the worker.
#
#   powershell -ExecutionPolicy Bypass -File install.ps1 [-Name NAME] [-InstallRoot DIR] [-NoAutostart] [-NoWorkspaces] [-Isolation off|fs]
#                                                 [-Gpu | -NoGpu] [-Update]
[CmdletBinding()]
param(
    [string]$Name = "",
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA "OxideSwarm"),
    [switch]$NoAutostart,
    [switch]$NoWorkspaces,
    [switch]$Gpu,
    [switch]$NoGpu,
    [switch]$Update,
    [ValidateSet('off', 'fs')]
    [string]$Isolation = ""
)
$ErrorActionPreference = 'Stop'
$Kit = $PSScriptRoot
function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
$DefaultRoot = Join-Path $env:LOCALAPPDATA 'OxideSwarm'
$PointerKey = 'HKCU:\Software\OxideSwarm\UsbKit'
$RunKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
$runValueName = if ($env:OXIDESWARM_USB_RUN_VALUE) { $env:OXIDESWARM_USB_RUN_VALUE } else { 'OxideSwarmUsbWorker' }
function FailAfterStart($msg) {
    if ($Isolation -eq 'fs') {
        Fail "$msg (if the worker already granted isolation ACLs, run 'Uninstall OxideSwarm.cmd' or uninstall.ps1 to revert them)"
    }
    Fail $msg
}

$Target = 'x86_64-pc-windows-msvc'
if (-not [Environment]::Is64BitOperatingSystem) { Fail "this kit needs 64-bit Windows" }
$TokenFile = Join-Path $Kit 'join.token'
# A kit made with make_kit.sh --no-autostart is meant for a trial on someone else's machine.
if (Test-Path (Join-Path $Kit 'no-autostart')) { $NoAutostart = $true }
$IsolationFile = Join-Path $Kit 'isolation'
if (-not $Isolation) {
    if (Test-Path $IsolationFile) {
        $bakedIso = (Get-Content -Raw $IsolationFile).Trim().ToLower()
        if ($bakedIso -notin @('off', 'fs')) { Fail "the kit's isolation file holds '$bakedIso' (expected 'off' or 'fs')" }
        $Isolation = $bakedIso
    } else {
        $Isolation = 'off'
    }
} else {
    $Isolation = $Isolation.ToLower()
}
$Sums = Join-Path $Kit 'archives\SHA256SUMS'
if (-not $Update -and -not (Test-Path $TokenFile)) { Fail "join.token is missing from the kit" }
if (-not (Test-Path $Sums)) { Fail "archives\SHA256SUMS is missing from the kit" }
$Archive = Get-ChildItem (Join-Path $Kit 'archives') -Filter "oxideswarm-*-$Target.zip" | Select-Object -First 1
if (-not $Archive) { Fail "no archive for $Target in the kit" }
# A kit made with make_kit.sh --gpu also holds the gpu-wgpu build, which uses this machine's GPU
# when it finds one. It is installed only when the person at the keyboard agrees (or -Gpu).
$GpuArchive = Get-ChildItem (Join-Path $Kit 'archives') -Filter "oxideswarm-*-$Target-gpu.zip" | Select-Object -First 1
if ($Update) {
    # The install to update: -InstallRoot, else the one the installer recorded, else the default.
    if (-not $PSBoundParameters.ContainsKey('InstallRoot')) {
        $recorded = $null
        try { $recorded = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $recorded = $null }
        if ($recorded) { $InstallRoot = $recorded }
    }
    $InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
    if (-not (Test-Path (Join-Path $InstallRoot 'bin\oxideswarm.exe'))) { Fail "there is no OxideSwarm from a USB kit in $InstallRoot; run 'Install OxideSwarm.cmd' first" }
    # Its settings: kit-install.txt (written since the update mode exists), else what is on disk.
    $settings = @{}
    $conf = Join-Path $InstallRoot 'kit-install.txt'
    if (Test-Path $conf) { foreach ($l in Get-Content $conf) { if ($l -match '^(\w+)=(.*)$') { $settings[$Matches[1]] = $Matches[2].Trim() } } }
    if (-not $Name) {
        if ($settings['name']) { $Name = $settings['name'] }
        else {
            $idFile = Get-ChildItem (Join-Path $InstallRoot 'home') -Filter 'worker_id.*' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
            if ($idFile) { $Name = $idFile.Name.Substring('worker_id.'.Length) }
        }
        if ($Name -notmatch '^[A-Za-z0-9_.-]+$') { $Name = "" }
    }
    if (-not $Gpu -and -not $NoGpu -and $settings.ContainsKey('gpu')) { if ($settings['gpu'] -eq '1') { $Gpu = $true } else { $NoGpu = $true } }
    if ($settings['workspaces'] -eq '0') { $NoWorkspaces = $true }
    if (-not $PSBoundParameters.ContainsKey('Isolation') -and $settings['isolation'] -in @('off', 'fs')) { $Isolation = $settings['isolation'] }
    # The start at logon stays as it is: only an install that has one gets it again.
    if (-not $PSBoundParameters.ContainsKey('NoAutostart')) {
        $run = $null
        try { $run = (Get-ItemProperty -Path $RunKey -Name $runValueName -ErrorAction Stop).$runValueName } catch { $run = $null }
        $NoAutostart = -not ($run -and $run.IndexOf($InstallRoot + '\', [StringComparison]::OrdinalIgnoreCase) -ge 0)
    }
}
if ($Gpu -and $NoGpu) { Fail "-Gpu and -NoGpu cannot be used together" }
if ($Gpu -and -not $GpuArchive) {
    if ($Update) { Fail "this install uses the GPU build, which this kit lacks; update from a kit made with make_kit.sh --gpu, or add -NoGpu" }
    Fail "this kit has no GPU build for $Target (make it with make_kit.sh --gpu)"
}

if ($Update) { Write-Host "OxideSwarm USB installer (update)" } else { Write-Host "OxideSwarm USB installer" }

# The install folder. The default lives in %LOCALAPPDATA%, which only this user can open. Any other
# folder must be on a fixed NTFS or ReFS drive (FAT and exFAT keep no permissions, and a removable
# drive can be pulled out mid-task), and is limited to this user, SYSTEM and Administrators below:
# a new folder at the root of a data drive otherwise lets every local account change its content.
$KitDrive = [IO.Path]::GetPathRoot($Kit)
function Test-InstallDrive($root) {
    # $null when $root may hold an install, else the reason it may not.
    if ($root -match '^\\\\') { return "network folders are not supported" }
    $driveRoot = [IO.Path]::GetPathRoot($root)
    if (-not $driveRoot) { return "use a full path such as D:\OxideSwarm" }
    if ($driveRoot -eq $KitDrive) { return "it is on the drive this kit runs from" }
    $drive = New-Object IO.DriveInfo $driveRoot
    if (-not $drive.IsReady) { return "drive $driveRoot is not ready" }
    if ($drive.DriveType -ne 'Fixed') { return "drive $driveRoot is a $($drive.DriveType) drive, not a fixed one" }
    if ($drive.DriveFormat -notin @('NTFS', 'ReFS')) { return "drive $driveRoot uses $($drive.DriveFormat), which keeps no permissions (NTFS or ReFS needed)" }
    # A cloud or virtual drive (Google Drive, rclone and the like) sits on no disk partition, and an
    # external USB disk reports itself as fixed but can be unplugged while a task runs.
    $letter = $driveRoot.TrimEnd('\')
    $parts = @(Get-CimInstance -Query "ASSOCIATORS OF {Win32_LogicalDisk.DeviceID='$letter'} WHERE AssocClass=Win32_LogicalDiskToPartition" -ErrorAction SilentlyContinue)
    if ($parts.Count -eq 0) { return "drive $driveRoot is not a partition of a local disk (a cloud or virtual drive)" }
    $disk = @(Get-CimInstance -Query ("ASSOCIATORS OF {Win32_DiskPartition.DeviceID='" + $parts[0].DeviceID + "'} WHERE AssocClass=Win32_DiskDriveToDiskPartition") -ErrorAction SilentlyContinue) | Select-Object -First 1
    if ($disk -and $disk.InterfaceType -eq 'USB') { return "drive $driveRoot is on an external USB disk, which can be unplugged while a task runs" }
    return $null
}
$explicitRoot = $PSBoundParameters.ContainsKey('InstallRoot')
if (-not $Update -and -not $explicitRoot -and -not [Console]::IsInputRedirected) {
    $choices = @($DefaultRoot)
    foreach ($d in [IO.DriveInfo]::GetDrives()) {
        if ($d.Name -eq [IO.Path]::GetPathRoot($DefaultRoot)) { continue }
        $candidate = Join-Path $d.Name 'OxideSwarm'
        if (-not (Test-InstallDrive $candidate)) { $choices += $candidate }
    }
    if ($choices.Count -gt 1) {
        Write-Host "Where should OxideSwarm keep its files (program, task folders, file cache)?"
        for ($i = 0; $i -lt $choices.Count; $i++) {
            $free = (New-Object IO.DriveInfo ([IO.Path]::GetPathRoot($choices[$i]))).AvailableFreeSpace / 1GB
            $note = if ($i -eq 0) { ' (default)' } else { '' }
            Write-Host ("  {0}) {1}{2}  {3:N1} GB free" -f ($i + 1), $choices[$i], $note, $free)
        }
        $pick = Read-Host "Choose [1]"
        if ($pick -match '^\s*(\d+)\s*$' -and [int]$Matches[1] -ge 1 -and [int]$Matches[1] -le $choices.Count) {
            $InstallRoot = $choices[[int]$Matches[1] - 1]
        } elseif ($pick.Trim()) {
            Fail "there is no choice '$pick'; nothing was installed"
        }
    }
}
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
$PrivateRoot = $InstallRoot.StartsWith($env:LOCALAPPDATA.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)
if (-not $PrivateRoot -and -not $Update) {
    if ([IO.Path]::GetPathRoot($InstallRoot + '\').TrimEnd('\') -eq $InstallRoot) { Fail "choose a folder such as $InstallRoot\OxideSwarm, not the root of a drive" }
    $why = Test-InstallDrive $InstallRoot
    if ($why) { Fail "cannot install in ${InstallRoot}: $why" }
    if (Test-Path -LiteralPath $InstallRoot) {
        $item = Get-Item -LiteralPath $InstallRoot -Force
        if (-not $item.PSIsContainer) { Fail "$InstallRoot is a file" }
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { Fail "$InstallRoot is a link; not using it" }
        $ours = (Test-Path (Join-Path $InstallRoot '.oxideswarm-usb-install')) -or (Test-Path (Join-Path $InstallRoot 'bin\oxideswarm.exe'))
        if (-not $ours -and (Get-ChildItem -LiteralPath $InstallRoot -Force | Select-Object -First 1)) {
            Fail "$InstallRoot already holds other files; choose a new or empty folder with -InstallRoot"
        }
    }
}
# One install per user: the uninstaller finds it through $PointerKey. An install somewhere else must
# be removed first, unless -InstallRoot asked for this folder on purpose.
$pointer = $null
try { $pointer = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $pointer = $null }
$previous = $pointer
if (-not $previous -and (Test-Path (Join-Path $DefaultRoot 'bin\oxideswarm.exe'))) { $previous = $DefaultRoot }
if (-not $Update -and $previous -and $previous -ne $InstallRoot -and (Test-Path (Join-Path $previous 'bin\oxideswarm.exe'))) {
    if (-not $explicitRoot) { Fail "OxideSwarm from a USB kit is already installed in $previous; run 'Uninstall OxideSwarm.cmd' first" }
    Write-Host "  note:    another install in $previous stays as it is"
}
Write-Host "  folder:  $InstallRoot"
if ($GpuArchive) {
    $useGpu = [bool]$Gpu
    if (-not $Gpu -and -not $NoGpu -and -not [Console]::IsInputRedirected) {
        $answer = Read-Host "Let the cluster use this computer's GPU as well? [Y/n]"
        $useGpu = -not ($answer -match '^\s*[Nn]')
    }
    if ($useGpu) {
        $Archive = $GpuArchive
        Write-Host "  gpu:     yes (GPU build; the worker offers the GPU it finds)"
    } else {
        Write-Host "  gpu:     no (CPU build; run install.ps1 -Gpu to change that)"
    }
}

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

# 2. Create the folder (limited to this user unless it is in %LOCALAPPDATA%) and unpack the binaries.
if (-not $PrivateRoot) {
    New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    & icacls.exe $InstallRoot /inheritance:r /grant:r "*${sid}:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "could not limit $InstallRoot to this user (icacls exit code $LASTEXITCODE)" }
    New-Item -ItemType Directory -Force -Path (Join-Path $InstallRoot 'work') | Out-Null
    Write-Host "  access:  this user, SYSTEM and Administrators only"
}
New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
Set-Content -Path (Join-Path $InstallRoot '.oxideswarm-usb-install') -Value 'Installed from an OxideSwarm USB kit; remove with Uninstall OxideSwarm.cmd.' -Encoding ASCII
if (-not ($pointer -and $pointer -ne $InstallRoot -and (Test-Path (Join-Path $pointer 'bin\oxideswarm.exe')))) {
    foreach ($k in @('HKCU:\Software\OxideSwarm', $PointerKey)) { if (-not (Test-Path $k)) { New-Item -Path $k | Out-Null } }
    Set-ItemProperty -Path $PointerKey -Name InstallRoot -Value $InstallRoot
}
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
    foreach ($revCand in @(
        (Join-Path $Kit 'revoke-isolation-acls.ps1'),
        (Join-Path $Kit '..\..\windows\revoke-isolation-acls.ps1')
    )) {
        if (Test-Path -LiteralPath $revCand) {
            Copy-Item -LiteralPath $revCand -Destination (Join-Path $InstallRoot 'revoke-isolation-acls.ps1') -Force
            break
        }
    }
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
$Exe = Join-Path $Bin 'oxideswarm.exe'
$version = & $Exe --version
if ($LASTEXITCODE -ne 0) { Fail "the unpacked binary does not run on this machine" }
Write-Host "  binary:  $version"

# Every command below keeps its state in $State and runs from $InstallRoot.
$GrantLog = Join-Path $State 'windows_acl_grants.txt'
# Task folders and the file cache: inside a folder outside %LOCALAPPDATA% (private, see step 2), else
# in the kit's own temp folder, which uninstall.ps1 removes.
$SandboxBase = if ($PrivateRoot) { Join-Path ([IO.Path]::GetTempPath()) 'oxideswarm-usb\sandboxes' } else { Join-Path $InstallRoot 'work' }
$RustupHome = if ($env:RUSTUP_HOME) { $env:RUSTUP_HOME } else { Join-Path $env:USERPROFILE '.rustup' }
$CargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $env:USERPROFILE '.cargo' }
$env:OXIDESWARM_HOME = $State
$env:RUSTY_GRID_ACL_GRANT_LOG = $GrantLog
$env:RUSTY_GRID_SANDBOX_BASE_DIR = $SandboxBase
Set-Location $InstallRoot

# 4. Join: the token goes in on standard input only. An update keeps the existing membership.
if (-not $Update) {
    Write-Host ""
    Write-Host "Joining the cluster ..."
    Get-Content -Raw $TokenFile | & $Exe join --no-doctor
    if ($LASTEXITCODE -ne 0) { Fail "join failed (see the message above); nothing was started" }
}

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
if ($ticket) {
    # Windows PowerShell 5.1 (and PowerShell 7 before 7.3) passes native arguments the legacy way
    # and drops the inner double quotes of the JSON ticket, so they must arrive escaped.
    $legacy = $true
    $mode = Get-Variable -Name PSNativeCommandArgumentPassing -ValueOnly -ErrorAction SilentlyContinue
    if ($mode -and $mode -ne 'Legacy') { $legacy = $false }
    $ticketArg = if ($legacy) { $ticket.Replace('"', '\"') } else { $ticket }
    $checkArgs = @('--master', $ticketArg)
    Write-Host "  link:    P2P ticket (the worker and the checks connect through iroh)"
}

# 5. Worker options.
if (-not $Name) { $Name = ($env:COMPUTERNAME).ToLower() -replace '[^a-z0-9_-]', '-' }
# A stable worker id for this machine and name in this cluster, so that a reinstall shows up as the
# same worker instead of a new one beside a disconnected old entry. It is a SHA-256 over the
# machine's own id (MachineGuid), the name and the cluster secret: the machine id never leaves the
# machine, and the same machine gets unrelated ids in different clusters. An existing id file is
# kept. OXIDESWARM_USB_MACHINE_ID replaces the machine id (tests).
$idFile = Join-Path $State "worker_id.$Name"
if (-not (Test-Path $idFile) -or (Get-Item $idFile).Length -eq 0) {
    $machineId = $env:OXIDESWARM_USB_MACHINE_ID
    if (-not $machineId) { try { $machineId = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Cryptography' -Name MachineGuid -ErrorAction Stop).MachineGuid } catch { $machineId = $null } }
    if (-not $machineId) { try { $machineId = (Get-CimInstance Win32_ComputerSystemProduct -ErrorAction Stop).UUID } catch { $machineId = $null } }
    if ($machineId) {
        $secretFile = Join-Path $State 'cluster.secret'
        $secretBytes = if (Test-Path $secretFile) { [IO.File]::ReadAllBytes($secretFile) } else { [byte[]]@() }
        $seed = [Text.Encoding]::UTF8.GetBytes("oxideswarm-usb-worker-id-v1`n$machineId`n$Name`n") + $secretBytes
        $h = [Security.Cryptography.SHA256]::Create().ComputeHash([byte[]]$seed)
        $h[6] = ($h[6] -band 0x0f) -bor 0x80   # UUID version 8 (vendor-specific)
        $h[8] = ($h[8] -band 0x3f) -bor 0x80   # RFC 9562 variant
        $hex = -join ($h[0..15] | ForEach-Object { $_.ToString('x2') })
        $uuid = '{0}-{1}-{2}-{3}-{4}' -f $hex.Substring(0, 8), $hex.Substring(8, 4), $hex.Substring(12, 4), $hex.Substring(16, 4), $hex.Substring(20, 12)
        [IO.File]::WriteAllText($idFile, $uuid + "`n")
    }
}
# Use at most half of the CPU cores, so the machine stays responsive for its owner.
$slots = [Math]::Max(1, [int][Math]::Floor([Environment]::ProcessorCount / 2))
$workerArgs = "worker --name $Name --no-portal --max-concurrency $slots --isolation $Isolation"
# Release 0.1.1 predates FEAT-3d (--workspaces under Windows --isolation fs), so omit --workspaces on 0.1.0/0.1.1 when -Isolation fs is used.
$supportsIsolatedWorkspaces = ($Isolation -eq 'off') -or ($version -notmatch '\b0\.1\.[01](\b|-)')
if (-not $NoWorkspaces -and $supportsIsolatedWorkspaces -and (Get-Command cargo -ErrorAction SilentlyContinue)) {
    $workerArgs += " --workspaces"
    # Outside %LOCALAPPDATA% the workspaces stay in the install folder, like the task folders ($SandboxBase).
    if (-not $PrivateRoot) { $workerArgs += ' --workspace-root "' + (Join-Path $InstallRoot 'workspaces') + '"' }
}
$log = Join-Path $LogDir 'worker.log'

# A small launcher that starts the worker hidden with this install's state folder. The HKCU Run
# entry uses it at every logon.
$vbs = Join-Path $InstallRoot 'start-worker.vbs'
$cmdLine = 'cmd /c ""' + $Exe + '" ' + $workerArgs + ' >> "' + $log + '" 2>&1"'
@(
    'Set sh = CreateObject("WScript.Shell")',
    ('sh.Environment("Process")("OXIDESWARM_HOME") = "' + $State + '"'),
    ('sh.Environment("Process")("HOME") = "' + $State + '"'),
    ('sh.Environment("Process")("RUSTUP_HOME") = "' + $RustupHome + '"'),
    ('sh.Environment("Process")("CARGO_HOME") = "' + $CargoHome + '"'),
    ('sh.Environment("Process")("RUSTY_GRID_ACL_GRANT_LOG") = "' + $GrantLog + '"'),
    ('sh.Environment("Process")("RUSTY_GRID_SANDBOX_BASE_DIR") = "' + $SandboxBase + '"'),
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
        Where-Object { $_.ExecutablePath -eq $Exe -and $_.CommandLine -match '(?:oxideswarm|rusty-grid)(?:\.exe)?["'']?\s+(worker)(\s|$)' } |
        Select-Object -First 1 -ExpandProperty ProcessId
}
if (-not $workerPid) { FailAfterStart "the worker did not start; see $log" }
$workerPid | Set-Content $pidFile
Start-Sleep -Seconds 3
if (-not (Get-Process -Id $workerPid -ErrorAction SilentlyContinue)) { FailAfterStart "the worker stopped right away; see $log" }

# 7. Wait until the master lists it. The checks run without log lines: release 0.1.x writes its
# informational logs to standard output, which would break the JSON parsing below. The worker
# was started above, so it keeps its own logging.
$env:RUST_LOG = 'off'
$connected = $false
for ($i = 0; $i -lt 30; $i++) {
    # The JSON listing carries the real state; the plain listing also shows disconnected workers.
    $list = $null
    try { $list = (& $Exe workers --json @checkArgs | Out-String) | ConvertFrom-Json } catch { $list = $null }
    if (@($list | Where-Object { $_.name -eq $Name -and $_.tags -contains 'state:connected' }).Count -gt 0) { $connected = $true; break }
    Start-Sleep -Seconds 1
}
if (-not $connected) { FailAfterStart "the worker runs but the master does not list it yet; see $log" }
Write-Host "  connected (isolation: $Isolation)"
# What an update keeps: the name, the build, the workspace choice and the isolation.
$builtWithGpu = if ($GpuArchive -and $Archive.FullName -eq $GpuArchive.FullName) { '1' } else { '0' }
$withWorkspaces = if ($NoWorkspaces) { '0' } else { '1' }
Set-Content -Path (Join-Path $InstallRoot 'kit-install.txt') -Encoding ASCII -Value @("name=$Name", "gpu=$builtWithGpu", "workspaces=$withWorkspaces", "isolation=$Isolation")

# 8. Start at logon (current user only, no administrator rights).
if (-not $NoAutostart) {
    Set-ItemProperty -Path $RunKey -Name $runValueName -Value ('wscript.exe "' + $vbs + '"')
    Write-Host "  autostart: HKCU Run entry $runValueName"
} else {
    Write-Host "  autostart: skipped (-NoAutostart)"
}

if ($Update) {
    Write-Host ""
    Write-Host "Updated. Worker '$Name' runs $(& $Exe --version) from $InstallRoot."
    exit 0
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
