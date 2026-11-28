# OxideSwarm USB installer for Windows (kit format v0 / v1, design 0053).
#
# Installs OxideSwarm for the current user from this USB stick (no administrator rights), joins
# the cluster with invite.oxs (v1) or join.token (v0), starts the worker hidden (when the kit
# includes the worker role), and registers an HKCU Run entry. The user's own
# %USERPROFILE%\.oxideswarm is never read or written: the installed worker keeps its state in
# <install folder>\home.
#
# The install folder is %LOCALAPPDATA%\OxideSwarm-USB unless -InstallRoot names another one or the
# person at the keyboard picks another fixed NTFS or ReFS drive. Workspaces stay in
# <install folder>\workspaces so the kit never shares the default worker workspace root
# (%LOCALAPPDATA%\oxideswarm\workspaces). The task folders and file cache go to <install folder>\work,
# except with -Isolation fs in %LOCALAPPDATA% (%TEMP%\oxideswarm-usb\sandboxes). Outside %LOCALAPPDATA%,
# the installer limits the install folder to this user, SYSTEM and Administrators.
#
# -Update (what "Update OxideSwarm.cmd" runs) updates an existing install in place: it keeps its
# folder, name, GPU choice, isolation, cluster membership and start at logon, asks nothing, does
# not join again, and only replaces the binaries and restarts the worker.
#
#   powershell -ExecutionPolicy Bypass -File install.ps1 [-Name NAME] [-InstallRoot DIR] [-NoAutostart] [-Workspaces | -NoWorkspaces] [-Isolation off|fs]
#                                                 [-Gpu | -NoGpu] [-Tools | -NoTools] [-Slots N] [-Update]
#
# -Slots N sets how many tasks the worker runs at once. Without it the installer uses the logical
# processors of the performance cores, at most half of all processors (USB-4).
[CmdletBinding()]
param(
    [string]$Name = "",
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA "OxideSwarm-USB"),
    [switch]$NoAutostart,
    [switch]$Workspaces,
    [switch]$NoWorkspaces,
    [switch]$Gpu,
    [switch]$NoGpu,
    [switch]$Tools,
    [switch]$NoTools,
    [switch]$Update,
    [int]$Slots = 0,
    [ValidateSet('off', 'fs')]
    [string]$Isolation = ""
)
$ErrorActionPreference = 'Stop'
$Kit = $PSScriptRoot
$script:PendingLogLines = New-Object 'System.Collections.Generic.List[string]'
$script:InstallLogPath = $null
$script:RollbackArmed = $false
$script:CreatedPointer = $false
$script:CreatedRunEntry = $false
$script:PreexistingRoot = $false
# Set once the worker is started: a rollback then keeps <install folder>\logs, so the log a failed
# start points to still exists (BUG-31).
$script:KeepLogs = $false
function Write-LogRecord([string]$line) {
    if ($script:InstallLogPath) {
        try { Add-Content -LiteralPath $script:InstallLogPath -Value $line -Encoding UTF8 } catch {}
    } else {
        $script:PendingLogLines.Add($line)
    }
}
function Say([string]$msg) {
    Write-Host $msg
    Write-LogRecord $msg
}
# Deletes $path, unless it is a link or holds one: Windows PowerShell 5.1 follows junctions while it
# deletes, and a task may have made one in its task folder.
function Remove-TreeNoFollow($path) {
    $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    if (-not $item) { return }
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { Write-Host "  kept ${path}: it is a link"; return }
    if ($item.PSIsContainer) {
        $link = @(Get-ChildItem -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint } | Select-Object -First 1)
        if ($link.Count -gt 0) { Write-Host "  kept ${path}: it holds a link ($($link[0].FullName))"; return }
    }
    Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue
}
function Rollback-FreshInstall {
    if (-not $script:RollbackArmed) { return }
    $script:RollbackArmed = $false
    try { Set-Location ([IO.Path]::GetTempPath()) } catch {}
    try {
        if ($script:CreatedRunEntry) {
            Remove-ItemProperty -Path $RunKey -Name $runValueName -ErrorAction SilentlyContinue
        }
    } catch {}
    try {
        if ($InstallRoot) {
            $rbExe = Join-Path $InstallRoot 'bin\oxideswarm.exe'
            Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.ExecutablePath -eq $rbExe } | ForEach-Object {
                Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
            }
            $rbPidFile = Join-Path $InstallRoot 'run\worker.pid'
            if (Test-Path -LiteralPath $rbPidFile) {
                $rbPid = Get-Content -LiteralPath $rbPidFile -ErrorAction SilentlyContinue
                if ($rbPid -match '^\d+$') {
                    $rbProc = Get-Process -Id ([int]$rbPid) -ErrorAction SilentlyContinue
                    if ($rbProc -and $rbProc.Path -eq $rbExe) {
                        Stop-Process -Id ([int]$rbPid) -Force -ErrorAction SilentlyContinue
                    }
                }
            }
            Start-Sleep -Milliseconds 300
        }
    } catch {}
    try {
        if ($script:CreatedPointer) {
            Remove-Item -Path $PointerKey -Force -ErrorAction SilentlyContinue
        }
    } catch {}
    try {
        if ($InstallRoot -and (Test-Path -LiteralPath $InstallRoot)) {
            # After a failed worker start the logs stay, so the error can name them (BUG-31).
            $logsDir = Join-Path $InstallRoot 'logs'
            $keepLogs = $script:KeepLogs -and (Test-Path -LiteralPath $logsDir)
            if (-not $script:PreexistingRoot -and -not $keepLogs) {
                Remove-TreeNoFollow $InstallRoot
            } elseif (-not $script:PreexistingRoot) {
                foreach ($c in @(Get-ChildItem -LiteralPath $InstallRoot -Force -ErrorAction SilentlyContinue)) {
                    if ($c.Name -eq 'logs') { continue }
                    Remove-TreeNoFollow $c.FullName
                }
            } else {
                foreach ($f in @('.oxideswarm-usb-install', 'kit-install.txt', 'start-worker.vbs', 'revoke-isolation-acls.ps1')) {
                    $p = Join-Path $InstallRoot $f
                    if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }
                }
                foreach ($d in @('bin', 'home', 'run', 'logs', 'workspaces', 'work', 'tools', 'cargo-home')) {
                    if ($d -eq 'logs' -and $keepLogs) { continue }
                    $dp = Join-Path $InstallRoot $d
                    if (Test-Path -LiteralPath $dp) { Remove-TreeNoFollow $dp }
                }
            }
            if ($keepLogs) { Write-Host "The logs of this attempt stay in $logsDir; delete that folder when you no longer need them." }
        }
    } catch {}
}
function Fail($msg) {
    Write-LogRecord "ERROR: $msg"
    Rollback-FreshInstall
    Write-Host "ERROR: $msg" -ForegroundColor Red
    exit 1
}
if ($Workspaces -and $NoWorkspaces) { Fail "cannot specify both -Workspaces and -NoWorkspaces" }
if ($Tools -and $NoTools) { Fail "cannot specify both -Tools and -NoTools" }
if ($PSBoundParameters.ContainsKey('Slots') -and ($Slots -lt 1 -or $Slots -gt 256)) { Fail "-Slots needs a number from 1 to 256 (got: $Slots)" }
$slotsSet = if ($PSBoundParameters.ContainsKey('Slots')) { $Slots } else { 0 }
# Full form of %LOCALAPPDATA%: GetFullPath on Windows PowerShell 5.1 expands 8.3 short names (a CI
# runner's C:\Users\RUNNER~1), so both sides of every comparison below go through it.
$LocalAppDataFull = [IO.Path]::GetFullPath($env:LOCALAPPDATA).TrimEnd('\')
$DefaultRoot = Join-Path $LocalAppDataFull 'OxideSwarm-USB'
$LegacyDefaultRoot = Join-Path $LocalAppDataFull 'OxideSwarm'
$DefaultWorkerWorkspaceRoot = Join-Path $LegacyDefaultRoot 'workspaces'
$PointerKey = if ($env:OXIDESWARM_USB_POINTER_KEY) { $env:OXIDESWARM_USB_POINTER_KEY } else { 'HKCU:\Software\OxideSwarm\UsbKit' }
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
$InviteFile = Join-Path $Kit 'invite.oxs'
$KitJson = Join-Path $Kit 'kit.json'
$IsV1Kit = Test-Path -LiteralPath $InviteFile
$kitVersion = $null
$KitHasWorker = $true
$KitHasClient = $true
$useWorkspaces = $true
$kitToolset = @()
if ($IsV1Kit -and -not (Test-Path -LiteralPath $KitJson)) { Fail "kit.json is missing from this v1 kit" }
if (Test-Path -LiteralPath $KitJson) {
    try { $kitMeta = Get-Content -Raw -LiteralPath $KitJson | ConvertFrom-Json }
    catch { Fail "invalid kit.json: $_" }
    if ($IsV1Kit) {
        if ($kitMeta.kit_format -ne 1) { Fail "unsupported kit_format '$($kitMeta.kit_format)' in kit.json (expected 1)" }
    } elseif ($kitMeta.kit_format -notin @(0, 1)) {
        Fail "unsupported kit_format '$($kitMeta.kit_format)' in kit.json (expected 0 or 1)"
    }
    $kitVersion = "$($kitMeta.version)"
    if (-not $kitVersion -or $kitVersion.Contains('..') -or $kitVersion -notmatch '^[A-Za-z0-9.+_-]+$') {
        Fail "invalid version '$kitVersion' in kit.json"
    }
    $kitRoles = @($kitMeta.roles | ForEach-Object { "$_".ToLowerInvariant() })
    $KitHasWorker = $kitRoles -contains 'worker'
    $KitHasClient = $kitRoles -contains 'client'
    if (-not $KitHasWorker -and -not $KitHasClient) { Fail "kit.json roles lists neither 'worker' nor 'client'" }
    if (-not $Update -and $kitMeta.PSObject.Properties.Name -contains 'workspaces') { $useWorkspaces = [bool]$kitMeta.workspaces }
    if ($kitMeta.PSObject.Properties.Name -contains 'toolset' -and $kitMeta.toolset) {
        $seenTools = New-Object 'System.Collections.Generic.List[string]'
        foreach ($rt in @($kitMeta.toolset)) {
            $tn = "$rt".Trim().ToLowerInvariant()
            if ($tn -notin @('python', 'rust', 'mingw', 'git')) { Fail "unsupported tool '$rt' in kit.json toolset" }
            if (-not $seenTools.Contains($tn)) { $seenTools.Add($tn) }
        }
        $kitToolset = @($seenTools)
    }
}
$KitToolsLock = Join-Path $Kit 'tools\tools.lock'
$KitNoticeFile = Join-Path $Kit 'tools\NOTICE.txt'
$kitToolEntries = @()
if ($kitToolset.Count -gt 0) {
    if (-not (Test-Path -LiteralPath $KitToolsLock)) { Fail "tools\tools.lock is missing from this toolset kit" }
    if (-not (Test-Path -LiteralPath $KitNoticeFile)) { Fail "tools\NOTICE.txt is missing from this toolset kit" }
    foreach ($lockLine in (Get-Content -LiteralPath $KitToolsLock)) {
        $trimmedLine = $lockLine.Trim()
        if (-not $trimmedLine -or $trimmedLine.StartsWith('#')) { continue }
        $cols = $trimmedLine -split "`t"
        if ($cols.Count -ne 6) { Fail "malformed line in tools\tools.lock: $trimmedLine" }
        $tName = $cols[0].Trim().ToLowerInvariant()
        if ($tName -notin $kitToolset) { continue }
        $tFile = $cols[3].Trim()
        $tSha = $cols[4].Trim().ToLowerInvariant()
        if (-not $tFile -or $tFile.Contains('/') -or $tFile.Contains('\') -or $tFile.Contains('..')) {
            Fail "unsafe file name '$tFile' in tools\tools.lock"
        }
        if ($tSha -notmatch '^[0-9a-f]{64}$') { Fail "invalid sha256 '$tSha' in tools\tools.lock for $tFile" }
        $kitToolEntries += [pscustomobject]@{
            Tool      = $tName
            Component = $cols[1].Trim().ToLowerInvariant()
            Version   = $cols[2].Trim()
            FileName  = $tFile
            Sha256    = $tSha
            Url       = $cols[5].Trim()
        }
    }
    foreach ($reqTool in $kitToolset) {
        if (@($kitToolEntries | Where-Object { $_.Tool -eq $reqTool }).Count -eq 0) {
            Fail "tools\tools.lock has no entries for '$reqTool'"
        }
    }
}
if (-not $KitHasWorker) {
    $NoGpu = $true
    if ($Tools) { Fail "-Tools requires a kit with the worker role" }
    $NoTools = $true
}
if (-not $Update -and $Tools -and $kitToolset.Count -eq 0) {
    Fail "this kit has no developer toolset (make it with make_kit.sh --toolset full)"
}
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
if (-not $IsV1Kit) {
    if (-not $Update -and -not (Test-Path $TokenFile)) { Fail "join.token (or invite.oxs) is missing from the kit" }
}
if (-not (Test-Path $Sums)) { Fail "archives\SHA256SUMS is missing from the kit" }
$Archive = $null
if ($IsV1Kit -and $kitVersion) {
    $exactArch = Join-Path $Kit "archives\oxideswarm-$kitVersion-$Target.zip"
    if (Test-Path -LiteralPath $exactArch) { $Archive = Get-Item -LiteralPath $exactArch }
}
if (-not $Archive) {
    $Archive = Get-ChildItem (Join-Path $Kit 'archives') -Filter "oxideswarm-*-$Target.zip" | Select-Object -First 1
}
if (-not $Archive) { Fail "no archive for $Target in the kit" }
# A kit made with make_kit.sh --gpu also holds the gpu-wgpu build, which uses this machine's GPU
# when it finds one. It is installed only when the person at the keyboard agrees (or -Gpu).
$GpuArchive = $null
if ($IsV1Kit -and $kitVersion) {
    $exactGpu = Join-Path $Kit "archives\oxideswarm-$kitVersion-$Target-gpu.zip"
    if (Test-Path -LiteralPath $exactGpu) { $GpuArchive = Get-Item -LiteralPath $exactGpu }
}
if (-not $GpuArchive) {
    $GpuArchive = Get-ChildItem (Join-Path $Kit 'archives') -Filter "oxideswarm-*-$Target-gpu.zip" | Select-Object -First 1
}
if ($Update) {
    # The install to update: -InstallRoot, else the one the installer recorded, else the default
    # (%LOCALAPPDATA%\OxideSwarm-USB), falling back to legacy %LOCALAPPDATA%\OxideSwarm.
    if (-not $PSBoundParameters.ContainsKey('InstallRoot')) {
        $recorded = $null
        try { $recorded = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $recorded = $null }
        if ($recorded) {
            $InstallRoot = $recorded
        } elseif (-not (Test-Path -LiteralPath (Join-Path $DefaultRoot 'bin\oxideswarm.exe')) -and (Test-Path -LiteralPath (Join-Path $LegacyDefaultRoot 'bin\oxideswarm.exe'))) {
            $InstallRoot = $LegacyDefaultRoot
        }
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
    if (-not $Tools -and -not $NoTools -and $settings.ContainsKey('tools')) { if ($settings['tools'] -eq '1') { $Tools = $true } else { $NoTools = $true } }
    if ($settings['workspaces'] -eq '0') { $useWorkspaces = $false }
    elseif ($settings['workspaces'] -eq '1') { $useWorkspaces = $true }
    if (-not $IsV1Kit -and $settings.ContainsKey('worker')) { $KitHasWorker = ($settings['worker'] -eq '1') }
    if (-not $IsV1Kit -and $settings.ContainsKey('client')) { $KitHasClient = ($settings['client'] -eq '1') }
    if (-not $KitHasWorker) { $NoTools = $true }
    # Slots chosen with -Slots at install time are kept; an automatic choice is made again.
    if ($slotsSet -eq 0 -and $settings['slots'] -match '^\d+$' -and [int]$settings['slots'] -ge 1 -and [int]$settings['slots'] -le 256) { $slotsSet = [int]$settings['slots'] }
    # The isolation it runs with is kept: kit-install.txt, else the --isolation in its launcher (an
    # install made before kit-install.txt existed). An update never lowers it without -Isolation.
    $recordedIso = $settings['isolation']
    if (-not $recordedIso) {
        $oldVbs = Join-Path $InstallRoot 'start-worker.vbs'
        if ((Test-Path -LiteralPath $oldVbs) -and ((Get-Content -Raw -LiteralPath $oldVbs) -match '--isolation (off|fs)\b')) { $recordedIso = $Matches[1] }
    }
    if (-not $PSBoundParameters.ContainsKey('Isolation') -and $recordedIso -in @('off', 'fs')) { $Isolation = $recordedIso }
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
# An install folder outside %LOCALAPPDATA% gets a DACL this installer builds itself: protected (no
# inherited entries), full control for this user, SYSTEM and Administrators only, owned by this user.
# Another local account must not be able to read the cluster secret in it, or replace the worker
# that the start-at-logon entry runs as this user.
$UserSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
# Non-elevated tokens own new objects as $UserSid; elevated admin tokens default to TokenOwner (S-1-5-32-544).
$OwnerSids = @($UserSid.Value, [Security.Principal.WindowsIdentity]::GetCurrent().Owner.Value) | Select-Object -Unique
$TrustedSids = @($UserSid.Value, 'S-1-5-18', 'S-1-5-32-544')
$TrustedInstaller = 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'
function Get-PrivateSecurity {
    $sec = New-Object System.Security.AccessControl.DirectorySecurity
    $sec.SetAccessRuleProtection($true, $false)
    foreach ($sid in $TrustedSids) {
        $sec.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier $sid), 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow')))
    }
    return $sec
}
function Get-AccountName($sid) {
    try { return (New-Object Security.Principal.SecurityIdentifier $sid).Translate([Security.Principal.NTAccount]).Value } catch { return $sid }
}
function Test-SidResolvable($sid) {
    try { [void](New-Object Security.Principal.SecurityIdentifier $sid).Translate([Security.Principal.NTAccount]); return $true } catch { return $false }
}
# The first account other than this user, SYSTEM, Administrators, TrustedInstaller or an app package
# that owns $path or is granted one of the $mask rights on it; $null when there is none. Entries that
# only flow to children (inherit-only) do not count for $path itself.
function Get-ForeignGrant($path, [int]$mask, [bool]$inherited = $true) {
    $acl = Get-Acl -LiteralPath $path
    $owner = $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
    if ($owner -notin $TrustedSids -and $owner -ne $TrustedInstaller) { return "owner $(Get-AccountName $owner)" }
    foreach ($r in $acl.GetAccessRules($true, $inherited, [Security.Principal.SecurityIdentifier])) {
        if ($r.AccessControlType -ne 'Allow') { continue }
        if ($r.PropagationFlags -band [System.Security.AccessControl.PropagationFlags]::InheritOnly) { continue }
        $sid = $r.IdentityReference.Value
        if ($sid -in $TrustedSids -or $sid -eq $TrustedInstaller -or $sid -like 'S-1-15-*') { continue }
        # GENERIC_ALL (0x10000000) holds every right.
        if (([int]$r.FileSystemRights -band $mask) -or ([int]$r.FileSystemRights -band 0x10000000)) { return Get-AccountName $sid }
    }
    return $null
}
# Whether $acl is the private DACL: protected, and only trusted accounts (app package SIDs that task
# isolation adds are allowed).
function Test-PrivateAcl($acl) {
    if (-not $acl.AreAccessRulesProtected) { return $false }
    foreach ($r in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
        $sid = $r.IdentityReference.Value
        if ($r.AccessControlType -eq 'Allow' -and $sid -notin $TrustedSids -and $sid -notlike 'S-1-15-*') { return $false }
    }
    return $true
}
# Sets $sec's DACL on an existing directory via a single handle opened with
# FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, refusing links and foreign owners on
# that handle before touching any ACL.
function Set-PrivateDaclNoFollow($path, $sec, [string[]]$expectedOwnerSids) {
    if (-not ('OxideSwarmDirAcl' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.AccessControl;

public static class OxideSwarmDirAcl {
    const uint READ_CONTROL = 0x00020000;
    const uint WRITE_DAC = 0x00040000;
    const uint FILE_SHARE_ALL = 0x00000007;
    const uint OPEN_EXISTING = 3;
    const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
    const uint FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;
    const uint FILE_ATTRIBUTE_DIRECTORY = 0x00000010;
    const uint FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400;
    const uint OWNER_SECURITY_INFORMATION = 0x00000001;
    const uint DACL_SECURITY_INFORMATION = 0x00000004;

    [StructLayout(LayoutKind.Sequential)]
    struct FILETIME { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    struct BY_HANDLE_FILE_INFORMATION {
        public uint dwFileAttributes;
        public FILETIME ftCreationTime;
        public FILETIME ftLastAccessTime;
        public FILETIME ftLastWriteTime;
        public uint dwVolumeSerialNumber;
        public uint nFileSizeHigh;
        public uint nFileSizeLow;
        public uint nNumberOfLinks;
        public uint nFileIndexHigh;
        public uint nFileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr CreateFileW(string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetFileInformationByHandle(IntPtr hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool CloseHandle(IntPtr hObject);

    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool GetKernelObjectSecurity(IntPtr Handle, uint RequestedInformation, byte[] pSecurityDescriptor, uint nLength, out uint lpnLengthNeeded);

    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool SetKernelObjectSecurity(IntPtr Handle, uint SecurityInformation, byte[] SecurityDescriptor);

    public static string Apply(string path, byte[] sdBytes, string[] expectedOwnerSids) {
        IntPtr h = CreateFileW(path, READ_CONTROL | WRITE_DAC, FILE_SHARE_ALL, IntPtr.Zero, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, IntPtr.Zero);
        if (h == new IntPtr(-1)) throw new Win32Exception(Marshal.GetLastWin32Error());
        try {
            BY_HANDLE_FILE_INFORMATION info;
            if (!GetFileInformationByHandle(h, out info)) throw new Win32Exception(Marshal.GetLastWin32Error());
            if ((info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) return "link";
            if ((info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0) return "file";
            uint needed = 0;
            GetKernelObjectSecurity(h, OWNER_SECURITY_INFORMATION, null, 0, out needed);
            if (needed == 0) throw new Win32Exception(Marshal.GetLastWin32Error());
            byte[] ownerBuf = new byte[needed];
            if (!GetKernelObjectSecurity(h, OWNER_SECURITY_INFORMATION, ownerBuf, needed, out needed)) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            RawSecurityDescriptor ownerSd = new RawSecurityDescriptor(ownerBuf, 0);
            string ownerSid = ownerSd.Owner != null ? ownerSd.Owner.Value : "";
            bool ownerOk = false;
            if (expectedOwnerSids != null) {
                foreach (string allowed in expectedOwnerSids) {
                    if (string.Equals(ownerSid, allowed, StringComparison.OrdinalIgnoreCase)) {
                        ownerOk = true;
                        break;
                    }
                }
            }
            if (!ownerOk) {
                return "owner:" + ownerSid;
            }
            if (!SetKernelObjectSecurity(h, DACL_SECURITY_INFORMATION, sdBytes)) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            return null;
        } finally {
            CloseHandle(h);
        }
    }
}
'@
    }
    return [OxideSwarmDirAcl]::Apply($path, $sec.GetSecurityDescriptorBinaryForm(), $expectedOwnerSids)
}
# Entries under an existing open $root to inspect: direct children of $root, the installer's own
# small control folders (bin, home, run, logs), and the worker's cache/lock directories under work.
# Skips task sandboxes, workspace trees and cargo target trees so -Update stays fast and junctions
# created by tasks inside sandboxes or workspaces do not block an update.
function Get-CheckedRootEntries($root, [bool]$ours) {
    $direct = @(Get-ChildItem -LiteralPath $root -Force -ErrorAction Stop)
    if (-not $ours) { return $direct }
    $entries = New-Object 'System.Collections.Generic.List[System.IO.FileSystemInfo]'
    foreach ($d in $direct) {
        $entries.Add($d)
        if (-not $d.PSIsContainer -or ($d.Attributes -band [IO.FileAttributes]::ReparsePoint)) { continue }
        $leaf = $d.Name.ToLowerInvariant()
        if ($leaf -in @('bin', 'home', 'run', 'logs')) {
            foreach ($sub in @(Get-ChildItem -LiteralPath $d.FullName -Recurse -Force -ErrorAction Stop)) {
                $entries.Add($sub)
            }
        } elseif ($leaf -eq 'work') {
            foreach ($rel in @('.locks', 'cache', 'cache\blobs', 'cache\blobs\objects', 'cache\blobs\tmp')) {
                $p = Join-Path $d.FullName $rel
                if (Test-Path -LiteralPath $p) {
                    $entries.Add((Get-Item -LiteralPath $p -Force -ErrorAction Stop))
                }
            }
        } elseif ($leaf -eq 'workspaces') {
            foreach ($ws in @(Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction Stop)) {
                $entries.Add($ws)
            }
        } else {
            foreach ($sub in @(Get-ChildItem -LiteralPath $d.FullName -Force -ErrorAction Stop)) {
                $entries.Add($sub)
            }
        }
    }
    return @($entries)
}
function Format-RootLinkRefusal($root, $linkPath) {
    return "$linkPath is a link; remove that link (keep $(Join-Path $root 'home') in place to preserve cluster membership) and run the installer again"
}
# DELETE 0x10000, WRITE_DAC 0x40000, WRITE_OWNER 0x80000, FILE_DELETE_CHILD 0x40.
$RenameRights = 0x10000 -bor 0x40000 -bor 0x80000 -bor 0x40
# Why $root may not be used: nobody else may rename, delete or re-permission a folder on the way to
# it (or delete folders at the drive root), and an existing $root must belong to this user and hold
# nothing another account wrote or may change.
function Test-RootSafety($root) {
    $drive = [IO.Path]::GetPathRoot($root)
    $dir = Split-Path -Parent $root
    while ($dir -and $dir.TrimEnd('\') -ne $drive.TrimEnd('\')) {
        if (Test-Path -LiteralPath $dir) {
            if ((Get-Item -LiteralPath $dir -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) { return "$dir is a link" }
            $who = Get-ForeignGrant $dir $RenameRights
            if ($who) { return "another account ($who) can rename or change $dir; choose a folder directly under the drive, such as $(Join-Path $drive 'OxideSwarm')" }
        }
        $dir = Split-Path -Parent $dir
    }
    $who = Get-ForeignGrant $drive (0x40 -bor 0x40000 -bor 0x80000)
    if ($who) { return "another account ($who) can delete or re-permission folders at the root of $drive" }
    if (-not (Test-Path -LiteralPath $root)) { return $null }
    $item = Get-Item -LiteralPath $root -Force
    if (-not $item.PSIsContainer) { return "$root is a file" }
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { return "$root is a link" }
    $acl = Get-Acl -LiteralPath $root
    $ownerSid = $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
    if ($ownerSid -notin $OwnerSids) {
        return "$root belongs to another account ($(Get-AccountName $ownerSid)); remove it or choose another folder"
    }
    if (Test-PrivateAcl $acl) { return $null }
    # Not made by this installer, or by an older one: every installer/control entry must be ours.
    $ours = (Test-Path -LiteralPath (Join-Path $root '.oxideswarm-usb-install')) -or (Test-Path -LiteralPath (Join-Path $root 'bin\oxideswarm.exe'))
    $children = @(Get-CheckedRootEntries $root $ours)
    if (-not $ours -and $children.Count -gt 0) { return "$root already holds other files; choose a new or empty folder with -InstallRoot" }
    foreach ($c in $children) {
        if ($c.Attributes -band [IO.FileAttributes]::ReparsePoint) { return (Format-RootLinkRefusal $root $c.FullName) }
        $who = Get-ForeignGrant $c.FullName -1 $false
        if ($who) { return "another account ($who) has rights on $($c.FullName); remove $root or choose another folder" }
    }
    return $null
}
# The worker's own check of its sandbox base (task folders and file cache; crates/worker/src/sandbox.rs,
# ensure_private_dir on Windows, with the masks of crates/core/src/private_file.rs), run before the
# worker starts so that a refusal names the folder and the accounts (BUG-31). It only reads
# permissions. DRIVE_ROOT_UNSAFE_MASK and DIR_WRITE_MASK; ANCESTOR_RENAME_MASK is $RenameRights.
$DriveRootRights = 0x40 -bor 0x40000 -bor 0x80000
$DirWriteRights = 0x2 -bor 0x4 -bor 0x10 -bor 0x40 -bor 0x100 -bor 0x10000 -bor 0x40000 -bor 0x80000 -bor 0x40000000
# Accounts other than this user, SYSTEM, Administrators, TrustedInstaller, CREATOR OWNER, OWNER RIGHTS
# or an app package that hold one of the $mask rights on $path itself, as @{ sid; name; inherited }
# (inherited: every such entry came from a folder above).
function Get-ForeignRights($path, [int]$mask) {
    $holders = New-Object 'System.Collections.Generic.List[object]'
    $acl = Get-Acl -LiteralPath $path
    foreach ($r in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
        if ($r.AccessControlType -ne 'Allow') { continue }
        if ($r.PropagationFlags -band [System.Security.AccessControl.PropagationFlags]::InheritOnly) { continue }
        $sid = $r.IdentityReference.Value
        if ($sid -in $TrustedSids -or $sid -eq $TrustedInstaller -or $sid -in @('S-1-3-0', 'S-1-3-4') -or $sid -like 'S-1-15-*') { continue }
        $rights = [int]$r.FileSystemRights
        if (-not (($rights -band $mask) -or ($rights -band 0x10000000))) { continue }
        $known = $null
        foreach ($h in $holders) { if ($h.sid -eq $sid) { $known = $h } }
        if ($known) {
            if (-not $r.IsInherited) { $known.inherited = $false }
        } else {
            $holders.Add(@{ sid = $sid; name = (Get-AccountName $sid); inherited = [bool]$r.IsInherited; resolvable = (Test-SidResolvable $sid) })
        }
    }
    return , $holders
}
# Why the worker would refuse $base as its sandbox base, as @{ dir; why; holders }, or $null: a folder
# on the way to it is a link, belongs to another account or lets another account rename, delete or
# re-permission it; another account may delete folders at the drive root; or an existing $base lets
# another account write in it.
function Test-SandboxBase($base) {
    $full = [IO.Path]::GetFullPath($base).TrimEnd('\')
    $drive = [IO.Path]::GetPathRoot($full)
    $chain = New-Object 'System.Collections.Generic.List[string]'
    $dir = Split-Path -Parent $full
    while ($dir -and $dir.TrimEnd('\') -ne $drive.TrimEnd('\')) {
        $chain.Insert(0, $dir)
        $dir = Split-Path -Parent $dir
    }
    $chain.Insert(0, $drive)
    foreach ($d in $chain) {
        if (-not (Test-Path -LiteralPath $d)) { break }
        $atDrive = ($d -eq $drive)
        if (-not $atDrive -and ((Get-Item -LiteralPath $d -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            return @{ dir = $d; why = "$d is a link"; holders = @() }
        }
        $owner = (Get-Acl -LiteralPath $d).GetOwner([Security.Principal.SecurityIdentifier]).Value
        if ($owner -notin $TrustedSids -and $owner -ne $TrustedInstaller) {
            return @{ dir = $d; why = "$d belongs to another account ($(Get-AccountName $owner))"; holders = @() }
        }
        $mask = if ($atDrive) { $DriveRootRights } else { $RenameRights }
        $holders = Get-ForeignRights $d $mask
        if ($holders.Count -gt 0) { return @{ dir = $d; why = $null; holders = $holders } }
    }
    if (Test-Path -LiteralPath $full) {
        $item = Get-Item -LiteralPath $full -Force
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { return @{ dir = $full; why = "$full is a link"; holders = @() } }
        if (-not $item.PSIsContainer) { return @{ dir = $full; why = "$full is a file"; holders = @() } }
        $owner = (Get-Acl -LiteralPath $full).GetOwner([Security.Principal.SecurityIdentifier]).Value
        if ($owner -notin $TrustedSids) {
            return @{ dir = $full; why = "$full belongs to another account ($(Get-AccountName $owner))"; holders = @() }
        }
        $holders = Get-ForeignRights $full $DirWriteRights
        if ($holders.Count -gt 0) { return @{ dir = $full; why = $null; holders = $holders } }
    }
    return $null
}
function Format-SandboxBaseProblem($problem) {
    if ($problem.holders.Count -eq 0) { return $problem.why }
    $names = (@($problem.holders) | ForEach-Object { $_.name }) -join ', '
    $tempFull = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')
    $dir = $problem.dir.TrimEnd('\')
    $what = $problem.dir
    if ([string]::Equals($dir, $tempFull, [StringComparison]::OrdinalIgnoreCase) -or $dir.StartsWith($tempFull + '\', [StringComparison]::OrdinalIgnoreCase)) {
        $what = "your Temp folder ($($problem.dir))"
    }
    return "$what lets these accounts change it: $names"
}
# The icacls command that removes the grants set on the folder itself. The installer only prints it:
# changing a user's permissions is the user's decision.
function Get-SandboxBaseFix($problem) {
    $explicit = @(@($problem.holders) | Where-Object { -not $_.inherited })
    if ($explicit.Count -eq 0) { return $null }
    $target = $problem.dir
    $hasUnresolvable = $false
    foreach ($h in $explicit) {
        $ok = if ($h.ContainsKey('resolvable')) { [bool]$h.resolvable } else { Test-SidResolvable $h.sid }
        if (-not $ok) { $hasUnresolvable = $true; break }
    }
    if ($hasUnresolvable) {
        $full = [IO.Path]::GetFullPath($target)
        $root = [IO.Path]::GetPathRoot($full)
        if ([string]::Equals($full.TrimEnd('\'), $root.TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase)) { return $null }
        return ('icacls "' + $target + '" /reset')
    }
    if ($target.EndsWith('\')) { $target += '\' }
    $sids = ($explicit | ForEach-Object { '"*' + $_.sid + '"' }) -join ' '
    return ('icacls "' + $target + '" /remove:g ' + $sids)
}
$explicitRoot = $PSBoundParameters.ContainsKey('InstallRoot')
# OXIDESWARM_USB_LIST_FOLDERS=1 prints the folder menu and stops; OXIDESWARM_USB_DRIVE_CHOICES (a
# ';'-separated list of folders) replaces the folders on the other drives, without the drive checks
# but with the root check (tests).
$listFoldersOnly = ($env:OXIDESWARM_USB_LIST_FOLDERS -eq '1')
if (-not $Update -and -not $explicitRoot -and ($listFoldersOnly -or -not [Console]::IsInputRedirected)) {
    $choices = @($DefaultRoot)
    $candidates = @()
    if ($env:OXIDESWARM_USB_DRIVE_CHOICES) {
        $candidates = @($env:OXIDESWARM_USB_DRIVE_CHOICES -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    } else {
        foreach ($d in [IO.DriveInfo]::GetDrives()) {
            if ($d.Name -eq [IO.Path]::GetPathRoot($DefaultRoot)) { continue }
            $candidate = Join-Path $d.Name 'OxideSwarm-USB'
            if (-not (Test-InstallDrive $candidate)) { $candidates += $candidate }
        }
    }
    # A folder is offered only when the install would accept it (BUG-29: the menu offered
    # E:\OxideSwarm-USB, and the install then refused it because another account could delete or
    # re-permission folders at the root of E:\). A refused one is listed, without a number, with the
    # reason.
    $refused = @()
    foreach ($candidate in $candidates) {
        $why = $null
        try { $why = Test-RootSafety $candidate } catch { $why = "its permissions could not be read ($($_.Exception.Message))" }
        if ($why) { $refused += , @($candidate, $why) } else { $choices += $candidate }
    }
    if ($choices.Count -gt 1 -or $refused.Count -gt 0 -or $listFoldersOnly) {
        Write-Host "Where should OxideSwarm keep its files (program, task folders, file cache)?"
        for ($i = 0; $i -lt $choices.Count; $i++) {
            $free = (New-Object IO.DriveInfo ([IO.Path]::GetPathRoot($choices[$i]))).AvailableFreeSpace / 1GB
            $note = if ($i -eq 0) { ' (default)' } else { '' }
            Write-Host ("  {0}) {1}{2}  {3:N1} GB free" -f ($i + 1), $choices[$i], $note, $free)
        }
        foreach ($r in $refused) { Write-Host ("  -  {0}  not usable: {1}" -f $r[0], $r[1]) }
    }
    if ($listFoldersOnly) { Write-Host "Nothing was installed (OXIDESWARM_USB_LIST_FOLDERS=1)."; exit 0 }
    if ($choices.Count -gt 1) {
        $pick = Read-Host "Choose [1]"
        if ($pick -match '^\s*(\d+)\s*$' -and [int]$Matches[1] -ge 1 -and [int]$Matches[1] -le $choices.Count) {
            $InstallRoot = $choices[[int]$Matches[1] - 1]
        } elseif ($pick.Trim()) {
            Fail "there is no choice '$pick'; nothing was installed"
        }
    }
}
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
# Keep fresh installs out of %LOCALAPPDATA%\OxideSwarm and its workspaces child, which is the
# default workspace root (%LOCALAPPDATA%\oxideswarm\workspaces) used by non-USB Windows workers.
if (-not $Update -and (
    [string]::Equals($InstallRoot, $LegacyDefaultRoot, [StringComparison]::OrdinalIgnoreCase) -or
    [string]::Equals($InstallRoot, $DefaultWorkerWorkspaceRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $InstallRoot.StartsWith($DefaultWorkerWorkspaceRoot + '\', [StringComparison]::OrdinalIgnoreCase)
)) {
    Fail "cannot install in ${InstallRoot}: $DefaultWorkerWorkspaceRoot is the default worker workspace root; omit -InstallRoot to use $DefaultRoot, or pass -Update to update an older install"
}
if ($NoWorkspaces) { $useWorkspaces = $false }
elseif ($Workspaces) { $useWorkspaces = $true }
$UserProfileFull = if ($env:USERPROFILE) { [IO.Path]::GetFullPath($env:USERPROFILE).TrimEnd('\') } else { '' }
$archivePredatesIsolatedWs = ($kitVersion -and $kitVersion -match '\b0\.1\.[01](\b|-)') -or ($Archive.Name -match '^oxideswarm-0\.1\.[01](-|\.)')
if ($KitHasWorker -and $Isolation -eq 'fs' -and $useWorkspaces -and (-not $archivePredatesIsolatedWs) -and ($Workspaces -or ($Tools -and ($kitToolset -contains 'rust')) -or [bool](Get-Command cargo -ErrorAction SilentlyContinue)) -and $UserProfileFull) {
    $plannedWsRoot = if ([string]::Equals($InstallRoot, $LegacyDefaultRoot, [StringComparison]::OrdinalIgnoreCase)) { Join-Path $InstallRoot 'usb-kit\workspaces' } else { Join-Path $InstallRoot 'workspaces' }
    $inUserProfile = [string]::Equals($plannedWsRoot, $UserProfileFull, [StringComparison]::OrdinalIgnoreCase) -or $plannedWsRoot.StartsWith($UserProfileFull + '\', [StringComparison]::OrdinalIgnoreCase)
    $allowedProfileWs1 = Join-Path $UserProfileFull 'AppData\Local\OxideSwarm-USB\workspaces'
    $allowedProfileWs2 = Join-Path $UserProfileFull 'AppData\Local\OxideSwarm\usb-kit\workspaces'
    $allowedInProfile = [string]::Equals($plannedWsRoot, $allowedProfileWs1, [StringComparison]::OrdinalIgnoreCase) -or [string]::Equals($plannedWsRoot, $allowedProfileWs2, [StringComparison]::OrdinalIgnoreCase)
    if ($inUserProfile -and -not $allowedInProfile) {
        Fail "cannot use ${InstallRoot} with -Isolation fs and workspaces: under -Isolation fs the worker accepts a workspace folder inside the user profile only in $DefaultRoot (or an updated legacy install in $LegacyDefaultRoot); omit -InstallRoot to use $DefaultRoot, or choose a folder outside $UserProfileFull"
    }
}
$script:PreexistingRoot = Test-Path -LiteralPath $InstallRoot
$freshInstall = (-not $Update) -and (-not (Test-Path -LiteralPath (Join-Path $InstallRoot 'bin\oxideswarm.exe'))) -and (-not (Test-Path -LiteralPath (Join-Path $InstallRoot 'home\node.key'))) -and (-not (Test-Path -LiteralPath (Join-Path $InstallRoot 'home\cluster.secret')))
$PrivateRoot = $InstallRoot.StartsWith($LocalAppDataFull + '\', [StringComparison]::OrdinalIgnoreCase)
if (-not $PrivateRoot) {
    if ([IO.Path]::GetPathRoot($InstallRoot + '\').TrimEnd('\') -eq $InstallRoot) { Fail "choose a folder such as $InstallRoot\OxideSwarm-USB, not the root of a drive" }
    if (-not $Update) {
        $why = Test-InstallDrive $InstallRoot
        if ($why) { Fail "cannot install in ${InstallRoot}: $why" }
    }
    $why = Test-RootSafety $InstallRoot
    if ($why) { Fail "cannot use ${InstallRoot}: $why" }
}
# One install per user: the uninstaller finds it through $PointerKey. An install somewhere else must
# be removed first, unless -InstallRoot asked for this folder on purpose.
$pointer = $null
try { $pointer = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $pointer = $null }
$previous = $pointer
if (-not $previous -and (Test-Path (Join-Path $DefaultRoot 'bin\oxideswarm.exe'))) { $previous = $DefaultRoot }
if (-not $previous -and (Test-Path (Join-Path $LegacyDefaultRoot 'bin\oxideswarm.exe'))) { $previous = $LegacyDefaultRoot }
$previousFull = if ($previous) { [IO.Path]::GetFullPath($previous).TrimEnd('\') } else { $null }
if (-not $Update -and $previousFull -and -not [string]::Equals($previousFull, $InstallRoot, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path (Join-Path $previousFull 'bin\oxideswarm.exe'))) {
    if (-not $explicitRoot) { Fail "OxideSwarm from a USB kit is already installed in $previousFull; run 'Uninstall OxideSwarm.cmd' first" }
    Write-Host "  note:    another install in $previousFull stays as it is"
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

# One question for the pinned private developer toolset (design 0110 D3): default Yes on a
# toolset kit; -Tools / -NoTools answer it without prompting; -Update keeps the earlier answer.
$useTools = $false
$selectedToolEntries = @()
if ($KitHasWorker -and $kitToolset.Count -gt 0) {
    $toolLabelParts = @()
    $displayNames = @{ python = 'Python'; rust = 'Rust'; mingw = 'MinGW'; git = 'Git' }
    foreach ($tn in $kitToolset) {
        $firstEntry = $kitToolEntries | Where-Object { $_.Tool -eq $tn } | Select-Object -First 1
        $shortVer = if ($firstEntry.Version -match '^(\d+\.\d+)') { $Matches[1] } else { $firstEntry.Version }
        $toolLabelParts += "$($displayNames[$tn]) $shortVer"
    }
    [int64]$totalToolBytes = 0
    foreach ($te in $kitToolEntries) {
        $tf = Join-Path $Kit ("tools\" + $te.FileName)
        if (-not (Test-Path -LiteralPath $tf)) { Fail "tool archive missing on stick: tools\$($te.FileName)" }
        $totalToolBytes += (Get-Item -LiteralPath $tf).Length
    }
    $toolSizeMb = [Math]::Max(1, [int][Math]::Ceiling($totalToolBytes / 1MB))
    $toolSummary = $toolLabelParts -join ', '
    if ($NoTools) {
        $useTools = $false
    } elseif ($Tools) {
        $useTools = $true
    } elseif (-not $Update -and -not [Console]::IsInputRedirected) {
        $toolAnswer = Read-Host "Install the developer tools for tasks ($toolSummary, $toolSizeMb MB on stick, only inside $InstallRoot)? [Y/n]"
        $useTools = -not ($toolAnswer -match '^\s*[Nn]')
    } else {
        $useTools = $true
    }
    if ($useTools) {
        $selectedToolEntries = @($kitToolEntries)
        Write-Host "  tools:   yes ($toolSummary, inside $InstallRoot\tools)"
    } else {
        Write-Host "  tools:   no (run install.ps1 -Tools to add them later)"
    }
}

if ($KitHasWorker -and $Isolation -eq 'fs' -and $useWorkspaces -and (-not $archivePredatesIsolatedWs) -and ($Workspaces -or ($useTools -and ($kitToolset -contains 'rust')) -or [bool](Get-Command cargo -ErrorAction SilentlyContinue)) -and $UserProfileFull) {
    $plannedWsRoot = if ([string]::Equals($InstallRoot, $LegacyDefaultRoot, [StringComparison]::OrdinalIgnoreCase)) { Join-Path $InstallRoot 'usb-kit\workspaces' } else { Join-Path $InstallRoot 'workspaces' }
    $inUserProfile = [string]::Equals($plannedWsRoot, $UserProfileFull, [StringComparison]::OrdinalIgnoreCase) -or $plannedWsRoot.StartsWith($UserProfileFull + '\', [StringComparison]::OrdinalIgnoreCase)
    $allowedProfileWs1 = Join-Path $UserProfileFull 'AppData\Local\OxideSwarm-USB\workspaces'
    $allowedProfileWs2 = Join-Path $UserProfileFull 'AppData\Local\OxideSwarm\usb-kit\workspaces'
    $allowedInProfile = [string]::Equals($plannedWsRoot, $allowedProfileWs1, [StringComparison]::OrdinalIgnoreCase) -or [string]::Equals($plannedWsRoot, $allowedProfileWs2, [StringComparison]::OrdinalIgnoreCase)
    if ($inUserProfile -and -not $allowedInProfile) {
        Fail "cannot use ${InstallRoot} with -Isolation fs and workspaces: under -Isolation fs the worker accepts a workspace folder inside the user profile only in $DefaultRoot (or an updated legacy install in $LegacyDefaultRoot); omit -InstallRoot to use $DefaultRoot, or choose a folder outside $UserProfileFull"
    }
}

# 1. Check the archive against the kit's SHA256SUMS (and online release SHA256SUMS when
#    configured or reachable).
function Lookup-SumEntry([string[]]$lines, [string]$fileName) {
    foreach ($line in $lines) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) { continue }
        $parts = $trimmed -split '\s+', 2
        if ($parts.Count -eq 2) {
            $hash = $parts[0].Trim()
            $namePart = $parts[1].Trim().TrimStart('*')
            if ($namePart -eq $fileName -and $hash -match '^[0-9a-fA-F]{64}$') {
                return $hash.ToLowerInvariant()
            }
        }
    }
    return $null
}

function Verify-OnlineSums([string]$fileName, [string]$localExpected) {
    $onlineUrl = $env:OXIDESWARM_USB_ONLINE_SUMS_URL
    $strict = $false
    if (-not $onlineUrl) {
        if (-not ($IsV1Kit -and $kitVersion)) { return }
        $onlineUrl = "https://github.com/DuongNAD/OxideSwarm/releases/download/v$kitVersion/SHA256SUMS"
        $strict = $false
    } elseif ($onlineUrl -match '^(?i)(off|0|none)$') {
        Say "  online:  skipped (OXIDESWARM_USB_ONLINE_SUMS_URL=off)"
        return
    } else {
        $strict = $true
    }

    $onlineUrl = $onlineUrl.Trim()
    [uri]$parsed = $null
    if (-not [uri]::TryCreate($onlineUrl, [UriKind]::Absolute, [ref]$parsed)) {
        Fail "invalid online SHA256SUMS URL '$onlineUrl'"
    }
    $onlineText = $null
    if ($parsed.Scheme -eq 'file') {
        $localPath = $parsed.LocalPath
        if (-not (Test-Path -LiteralPath $localPath)) {
            Fail "cannot read online SHA256SUMS from '$onlineUrl'"
        }
        $item = Get-Item -LiteralPath $localPath -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -or $item.PSIsContainer) {
            Fail "refusing symlink or directory at '$localPath'"
        }
        $onlineText = Get-Content -Raw -LiteralPath $localPath -Encoding UTF8
    } elseif ($parsed.Scheme -eq 'https') {
        if ($parsed.UserInfo) {
            Fail "refusing online SHA256SUMS URL with embedded credentials"
        }
        [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
        try {
            $req = [System.Net.HttpWebRequest]::Create($parsed)
            $req.Timeout = 10000
            $req.ReadWriteTimeout = 10000
            $req.UserAgent = 'OxideSwarm-USB-Installer'
            $resp = $req.GetResponse()
            try {
                if ($resp.ResponseUri.Scheme -ne 'https') {
                    Fail "refusing redirect to non-HTTPS URL '$($resp.ResponseUri)'"
                }
                $reader = New-Object System.IO.StreamReader($resp.GetResponseStream(), [System.Text.Encoding]::UTF8)
                try { $onlineText = $reader.ReadToEnd() } finally { $reader.Dispose() }
            } finally {
                $resp.Dispose()
            }
        } catch {
            if ($strict) {
                Fail "could not fetch online SHA256SUMS from '$onlineUrl': $_"
            } else {
                Say "  online:  unavailable (offline or unreleased v$kitVersion); relying on stick SHA256SUMS"
                return
            }
        }
    } else {
        Fail "refusing online SHA256SUMS URL '$onlineUrl': only https:// or file:// is allowed"
    }

    $onlineExpected = Lookup-SumEntry ($onlineText -split "`r?`n") $fileName
    if (-not $onlineExpected) {
        Fail "$fileName is not listed in online SHA256SUMS"
    }
    if ($onlineExpected -ne $localExpected) {
        Fail "online SHA256SUMS mismatch for $fileName (stick $localExpected vs online $onlineExpected); refusing tampered kit"
    }
    Say "  online:  $fileName matches online SHA256SUMS"
}

$sumLines = Get-Content $Sums
$expected = Lookup-SumEntry $sumLines $Archive.Name
if (-not $expected) { Fail "$($Archive.Name) is not listed in SHA256SUMS" }
$actual = (Get-FileHash $Archive.FullName -Algorithm SHA256).Hash.ToLower()
if ($expected -ne $actual) { Fail "checksum mismatch for $($Archive.Name): the copy on the stick is damaged" }
Say "  archive: $($Archive.Name) (checksum OK)"
Verify-OnlineSums $Archive.Name $expected

if ($selectedToolEntries.Count -gt 0) {
    foreach ($te in $selectedToolEntries) {
        $toolPath = Join-Path $Kit ("tools\" + $te.FileName)
        if (-not (Test-Path -LiteralPath $toolPath)) { Fail "tool archive missing on stick: tools\$($te.FileName)" }
        if ((Get-Item -LiteralPath $toolPath -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            Fail "refusing link at tools\$($te.FileName)"
        }
        $sumExp = Lookup-SumEntry $sumLines ("tools/" + $te.FileName)
        if (-not $sumExp) { $sumExp = Lookup-SumEntry $sumLines $te.FileName }
        if (-not $sumExp) { Fail "tools/$($te.FileName) is not listed in SHA256SUMS" }
        if ($sumExp -ne $te.Sha256) {
            Fail "SHA256SUMS disagrees with tools.lock for $($te.FileName) ($sumExp vs $($te.Sha256))"
        }
        $toolAct = (Get-FileHash -LiteralPath $toolPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($toolAct -ne $te.Sha256) {
            Fail "checksum mismatch for tools\$($te.FileName): the copy on the stick is damaged"
        }
    }
    Say "  tools:   all $($selectedToolEntries.Count) tool archive checksums OK"
}

# Task folders and the file cache (the worker's sandbox base): <install folder>\work, in %LOCALAPPDATA%
# too since 2026-11-18 (design 0053 status log). Only an install in %LOCALAPPDATA% with -Isolation fs
# keeps them in %TEMP%\oxideswarm-usb\sandboxes, which uninstall.ps1 removes: the isolated worker
# takes no other folder inside the user profile (validate_sandbox_base_safe). An update keeps the
# folder its launcher names while the worker can still use it.
$SandboxBase = if ($PrivateRoot -and $Isolation -eq 'fs') { Join-Path ([IO.Path]::GetTempPath()) 'oxideswarm-usb\sandboxes' } else { Join-Path $InstallRoot 'work' }
if ($Update -and $KitHasWorker) {
    $oldBase = $null
    $oldVbs = Join-Path $InstallRoot 'start-worker.vbs'
    if ((Test-Path -LiteralPath $oldVbs) -and ((Get-Content -Raw -LiteralPath $oldVbs) -match 'RUSTY_GRID_SANDBOX_BASE_DIR"\)\s*=\s*"([^"]+)"')) { $oldBase = $Matches[1] }
    if ($oldBase -and -not [string]::Equals($oldBase.TrimEnd('\'), $SandboxBase.TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase)) {
        $oldProblem = $null
        if ($PrivateRoot -and $Isolation -eq 'fs') {
            $oldProblem = "under -Isolation fs the worker takes task folders inside the user profile only from %TEMP%"
        } else {
            try { $p = Test-SandboxBase $oldBase; if ($p) { $oldProblem = Format-SandboxBaseProblem $p } } catch { $oldProblem = $null }
        }
        if ($oldProblem) { Say "  note:    task folders move from $oldBase to $SandboxBase ($oldProblem)" }
        else { $SandboxBase = $oldBase }
    }
}
# Check the folder the way the worker will, before anything is written, stopped or joined.
if ($KitHasWorker) {
    $baseProblem = $null
    try { $baseProblem = Test-SandboxBase $SandboxBase } catch { Say "  note:    could not read the permissions on the way to ${SandboxBase}: $($_.Exception.Message)" }
    if ($baseProblem) {
        $why = Format-SandboxBaseProblem $baseProblem
        Say "  The worker keeps its task folders in $SandboxBase and would refuse them: $why."
        $fix = Get-SandboxBaseFix $baseProblem
        if ($fix) {
            Say "  To remove those grants, run this yourself in a Command Prompt (this installer never changes permissions):"
            Say "    $fix"
        }
        if (@(@($baseProblem.holders) | Where-Object { $_.inherited }).Count -gt 0) {
            Say "  Some of these entries are inherited from a folder above $($baseProblem.dir); remove them there."
        }
        if ($PrivateRoot -and $Isolation -eq 'fs') {
            Say "  Or install without -Isolation fs (task folders then stay in $(Join-Path $InstallRoot 'work')), or on another drive with -InstallRoot."
        } else {
            Say "  Or choose a folder on another drive with -InstallRoot."
        }
        Fail "cannot start the worker: $why"
    }
    Write-Host "  tasks:   $SandboxBase"
}

# 2. Create the folder (limited to this user unless it is in %LOCALAPPDATA%) and unpack the binaries.
if (-not $PrivateRoot) {
    $sec = Get-PrivateSecurity
    $recheckExisting = $false
    $hadOurs = $false
    if (-not (Test-Path -LiteralPath $InstallRoot)) {
        # Created with its DACL in one call, so it never exists open to others.
        $sec.SetOwner($UserSid)
        [void][IO.Directory]::CreateDirectory($InstallRoot, $sec)
    } elseif (-not (Test-PrivateAcl (Get-Acl -LiteralPath $InstallRoot))) {
        $recheckExisting = $true
        $hadOurs = (Test-Path -LiteralPath (Join-Path $InstallRoot '.oxideswarm-usb-install')) -or (Test-Path -LiteralPath (Join-Path $InstallRoot 'bin\oxideswarm.exe'))
        $applyErr = Set-PrivateDaclNoFollow $InstallRoot $sec $OwnerSids
        if ($applyErr -eq 'link') { Fail "$InstallRoot is a link; remove it and run the installer again" }
        if ($applyErr -eq 'file') { Fail "$InstallRoot is a file" }
        if ($applyErr -like 'owner:*') {
            $badOwner = $applyErr.Substring('owner:'.Length)
            Fail "$InstallRoot belongs to another account ($(Get-AccountName $badOwner)); remove it or choose another folder"
        }
    }
    # CreateDirectory returns an existing folder as it is (one made in between by someone else), so
    # check what is there now.
    if ((Get-Item -LiteralPath $InstallRoot -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        Fail "$InstallRoot is a link; remove it and run the installer again"
    }
    $acl = Get-Acl -LiteralPath $InstallRoot
    if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $OwnerSids -or -not (Test-PrivateAcl $acl)) {
        Fail "could not make $InstallRoot private to this user; remove it and run the installer again"
    }
    if ($recheckExisting) {
        # Close the Test-RootSafety -> Set-PrivateDaclNoFollow window on an existing folder that was open to others.
        $afterChildren = @(Get-CheckedRootEntries $InstallRoot $hadOurs)
        if (-not $hadOurs -and $afterChildren.Count -gt 0) {
            Fail "files appeared in $InstallRoot before it could be made private; remove it or choose another folder"
        }
        foreach ($c in $afterChildren) {
            if ($c.Attributes -band [IO.FileAttributes]::ReparsePoint) { Fail (Format-RootLinkRefusal $InstallRoot $c.FullName) }
            $who = Get-ForeignGrant $c.FullName -1 $false
            if ($who) { Fail "another account ($who) has rights on $($c.FullName); remove $InstallRoot or choose another folder" }
        }
    }
    New-Item -ItemType Directory -Force -Path (Join-Path $InstallRoot 'work') | Out-Null
    Write-Host "  access:  this user, SYSTEM and Administrators only"
}
New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
$script:RollbackArmed = $freshInstall
Set-Content -LiteralPath (Join-Path $InstallRoot '.oxideswarm-usb-install') -Value 'Installed from an OxideSwarm USB kit; remove with Uninstall OxideSwarm.cmd.' -Encoding ASCII
$Bin = Join-Path $InstallRoot 'bin'
$State = Join-Path $InstallRoot 'home'
$RunDir = Join-Path $InstallRoot 'run'
$LogDir = Join-Path $InstallRoot 'logs'
foreach ($d in @($Bin, $State, $RunDir, $LogDir)) { New-Item -ItemType Directory -Force -Path $d | Out-Null }
$script:InstallLogPath = Join-Path $LogDir 'install.log'
Write-LogRecord ("=== OxideSwarm USB installer run ({0}) ===" -f ([DateTime]::UtcNow.ToString('o')))
foreach ($pendingLine in $script:PendingLogLines) { Write-LogRecord $pendingLine }
$script:PendingLogLines.Clear()
$Tmp = Join-Path ([IO.Path]::GetTempPath()) ("oxideswarm-usb-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Expand-Archive -Path $Archive.FullName -DestinationPath $Tmp -Force
    $src = Get-ChildItem $Tmp -Recurse -Filter 'oxideswarm.exe' | Select-Object -First 1
    if (-not $src) { Fail "the archive holds no oxideswarm.exe" }

    # 3. Stop a worker that an earlier run of this installer started (by its PID, never by name).
    $stopWaitMs = 15000
    if (-not [string]::IsNullOrWhiteSpace($env:OXIDESWARM_USB_STOP_WAIT_MS)) {
        $parsedStop = 0
        if ([int]::TryParse($env:OXIDESWARM_USB_STOP_WAIT_MS, [ref]$parsedStop) -and $parsedStop -ge 0) {
            $stopWaitMs = $parsedStop
        }
    }
    $copyWaitMs = 15000
    if (-not [string]::IsNullOrWhiteSpace($env:OXIDESWARM_USB_COPY_WAIT_MS)) {
        $parsedCopy = 0
        if ([int]::TryParse($env:OXIDESWARM_USB_COPY_WAIT_MS, [ref]$parsedCopy) -and $parsedCopy -ge 0) {
            $copyWaitMs = $parsedCopy
        }
    }
    $stoppedPids = New-Object System.Collections.Generic.List[int]
    $pidFile = Join-Path $RunDir 'worker.pid'
    if (Test-Path $pidFile) {
        $oldRaw = (Get-Content $pidFile -ErrorAction SilentlyContinue | Out-String).Trim()
        $oldPid = 0
        if ([int]::TryParse($oldRaw, [ref]$oldPid) -and $oldPid -gt 0) {
            $p = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
            if ($p -and $p.Path -and $p.Path -like "$Bin*") {
                Write-Host "  stopping the worker started earlier (PID $oldPid)"
                Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
                $stoppedPids.Add($oldPid)
            }
        }
    }
    # A worker started at logon by an earlier install has no PID file entry: match it by the exact
    # path of this install's binary, never by process name.
    $installedExe = Join-Path $Bin 'oxideswarm.exe'
    $cimProcs = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.ExecutablePath -eq $installedExe })
    foreach ($cp in $cimProcs) {
        $cpid = [int]$cp.ProcessId
        if ($cpid -gt 0 -and -not $stoppedPids.Contains($cpid)) {
            Write-Host "  stopping the worker started earlier (PID $cpid)"
            Stop-Process -Id $cpid -Force -ErrorAction SilentlyContinue
            $stoppedPids.Add($cpid)
        }
    }
    if ($stoppedPids.Count -gt 0) {
        $stopDeadline = [DateTime]::UtcNow.AddMilliseconds($stopWaitMs)
        $remainingPids = @($stoppedPids)
        while ($remainingPids.Count -gt 0) {
            $stillAlive = @()
            foreach ($spid in $remainingPids) {
                if (Get-Process -Id $spid -ErrorAction SilentlyContinue) {
                    $stillAlive += $spid
                }
            }
            if ($stillAlive.Count -eq 0) { break }
            if ([DateTime]::UtcNow -ge $stopDeadline) {
                $waitSecs = [Math]::Round($stopWaitMs / 1000.0, 1)
                Fail "the earlier worker (PID $($stillAlive -join ', ')) did not exit within ${waitSecs}s; stop it and re-run Update."
            }
            Start-Sleep -Milliseconds 250
            $remainingPids = $stillAlive
        }
    }
    if (Test-Path $pidFile) {
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
    foreach ($b in @('oxideswarm.exe', 'rusty-grid.exe', 'ox-mode.exe')) {
        $f = Join-Path $src.DirectoryName $b
        if (Test-Path $f) {
            $dst = Join-Path $Bin $b
            $copyDeadline = [DateTime]::UtcNow.AddMilliseconds($copyWaitMs)
            while ($true) {
                try {
                    Copy-Item -LiteralPath $f -Destination $dst -Force -ErrorAction Stop
                    break
                } catch {
                    $ex = $_.Exception
                    $msg = $ex.Message
                    $isLock = ($ex -is [System.IO.IOException]) -or
                              ($ex -is [System.UnauthorizedAccessException]) -or
                              ($ex.InnerException -is [System.IO.IOException]) -or
                              ($msg -match 'being used by another process|user-mapped section|access is denied|sharing violation')
                    if (-not $isLock) {
                        Fail "could not replace ${dst}: $msg"
                    }
                    if ([DateTime]::UtcNow -ge $copyDeadline) {
                        $waitSecs = [Math]::Round($copyWaitMs / 1000.0, 1)
                        Fail "could not replace $dst after waiting ${waitSecs}s (file still in use): $msg"
                    }
                    Start-Sleep -Milliseconds 250
                }
            }
        }
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
} catch {
    Fail "failed while unpacking $($Archive.Name): $_"
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
$Exe = Join-Path $Bin 'oxideswarm.exe'
$version = & $Exe --version
if ($LASTEXITCODE -ne 0) { Fail "the unpacked binary does not run on this machine" }
Write-Host "  binary:  $version"

$EnvCargoHome = $null
$ToolsDir = Join-Path $InstallRoot 'tools'
$PrivateCargoHome = Join-Path $InstallRoot 'cargo-home'
$installedToolDirs = @{}
$installedToolNames = @()
$oldToolDirsToClean = @()

function Expand-ToolArchive([string]$archivePath, [string]$destDir) {
    if (-not (Test-Path -LiteralPath $destDir)) {
        New-Item -ItemType Directory -Force -Path $destDir | Out-Null
    }
    if ($archivePath -match '\.zip$') {
        Expand-Archive -LiteralPath $archivePath -DestinationPath $destDir -Force
    } elseif ($archivePath -match '\.(tar\.xz|tar\.gz|tgz|txz|tar)$') {
        $global:LASTEXITCODE = 0
        & tar.exe -xf $archivePath -C $destDir
        if ($LASTEXITCODE -ne 0) { Fail "tar.exe failed to extract $archivePath (exit $LASTEXITCODE)" }
    } elseif ($archivePath -match '\.7z(\.exe)?$') {
        $global:LASTEXITCODE = 0
        & tar.exe -xf $archivePath -C $destDir 2>$null
        if ($LASTEXITCODE -ne 0) {
            if ($archivePath -match '\.7z\.exe$') {
                $p7 = Start-Process -FilePath $archivePath -ArgumentList @('-y', "-o$destDir") -Wait -WindowStyle Hidden -PassThru
                if ($p7.ExitCode -ne 0) { Fail "self-extracting archive $archivePath failed (exit $($p7.ExitCode))" }
            } else {
                Fail "tar.exe could not extract $archivePath (exit $LASTEXITCODE)"
            }
        }
    } else {
        Fail "unsupported tool archive format: $archivePath"
    }
}

function Copy-RelPathChecked([string]$srcBase, [string]$dstBase, [string]$relPath, [bool]$isDir) {
    $norm = $relPath.Trim().TrimStart('.', '/', '\').Replace('/', '\')
    if (-not $norm -or $norm.Contains('..') -or [IO.Path]::IsPathRooted($norm)) {
        Fail "unsafe relative path '$relPath' inside Rust component manifest"
    }
    $s = Join-Path $srcBase $norm
    $d = Join-Path $dstBase $norm
    if (-not (Test-Path -LiteralPath $s)) { Fail "manifest entry missing in extracted component: $norm" }
    if ($isDir) {
        New-Item -ItemType Directory -Force -Path $d | Out-Null
        foreach ($item in @(Get-ChildItem -LiteralPath $s -Recurse -Force)) {
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { continue }
            $subRel = $item.FullName.Substring($s.Length).TrimStart('\')
            $target = Join-Path $d $subRel
            if ($item.PSIsContainer) {
                if (-not (Test-Path -LiteralPath $target)) { New-Item -ItemType Directory -Force -Path $target | Out-Null }
            } else {
                $tp = Split-Path -Parent $target
                if (-not (Test-Path -LiteralPath $tp)) { New-Item -ItemType Directory -Force -Path $tp | Out-Null }
                [IO.File]::Copy($item.FullName, $target, $true)
            }
        }
    } else {
        $dp = Split-Path -Parent $d
        if (-not (Test-Path -LiteralPath $dp)) { New-Item -ItemType Directory -Force -Path $dp | Out-Null }
        [IO.File]::Copy($s, $d, $true)
    }
}

function Merge-RustComponentPackage([string]$pkgRoot, [string]$rustDest) {
    $compFile = Join-Path $pkgRoot 'components'
    if (Test-Path -LiteralPath $compFile) {
        $components = @(Get-Content -LiteralPath $compFile | ForEach-Object { $_.Trim() } | Where-Object { $_ })
        foreach ($comp in $components) {
            $compDir = Join-Path $pkgRoot $comp
            $manifestIn = Join-Path $compDir 'manifest.in'
            if (Test-Path -LiteralPath $manifestIn) {
                foreach ($line in (Get-Content -LiteralPath $manifestIn)) {
                    $m = $line.Trim()
                    if (-not $m) { continue }
                    if ($m.StartsWith('file:')) {
                        Copy-RelPathChecked $compDir $rustDest $m.Substring(5) $false
                    } elseif ($m.StartsWith('dir:')) {
                        Copy-RelPathChecked $compDir $rustDest $m.Substring(4) $true
                    }
                }
            } elseif (Test-Path -LiteralPath $compDir) {
                foreach ($item in @(Get-ChildItem -LiteralPath $compDir -Force)) {
                    if ($item.Name -eq 'manifest.in') { continue }
                    Copy-RelPathChecked $compDir $rustDest $item.Name $item.PSIsContainer
                }
            }
        }
    } else {
        foreach ($item in @(Get-ChildItem -LiteralPath $pkgRoot -Force)) {
            if ($item.Name -in @('install.sh', 'components', 'rust-installer-version', 'version')) { continue }
            Copy-RelPathChecked $pkgRoot $rustDest $item.Name $item.PSIsContainer
        }
    }
}

$GrantLog = Join-Path $State 'windows_acl_grants.txt'
$RustupHome = if ($env:RUSTUP_HOME) { $env:RUSTUP_HOME } else { Join-Path $env:USERPROFILE '.rustup' }
$CargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $env:USERPROFILE '.cargo' }
$toolchainCapSid = $null
if ($Isolation -eq 'fs') {
    if (-not ('OxideSwarmToolchainAcl' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;

public static class OxideSwarmToolchainAcl {
    const uint READ_CONTROL = 0x00020000;
    const uint WRITE_DAC = 0x00040000;
    const uint FILE_SHARE_ALL = 0x00000007;
    const uint OPEN_EXISTING = 3;
    const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
    const uint FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;
    const uint FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400;
    const uint DACL_SECURITY_INFORMATION = 0x00000004;

    [StructLayout(LayoutKind.Sequential)]
    struct FILETIME { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    struct BY_HANDLE_FILE_INFORMATION {
        public uint dwFileAttributes;
        public FILETIME ftCreationTime;
        public FILETIME ftLastAccessTime;
        public FILETIME ftLastWriteTime;
        public uint dwVolumeSerialNumber;
        public uint nFileSizeHigh;
        public uint nFileSizeLow;
        public uint nNumberOfLinks;
        public uint nFileIndexHigh;
        public uint nFileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr CreateFileW(string lpFileName, uint dwDesiredAccess, uint dwShareMode, IntPtr lpSecurityAttributes, uint dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetFileInformationByHandle(IntPtr hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool CloseHandle(IntPtr hObject);

    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool GetKernelObjectSecurity(IntPtr Handle, uint RequestedInformation, byte[] pSecurityDescriptor, uint nLength, out uint lpnLengthNeeded);

    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool SetKernelObjectSecurity(IntPtr Handle, uint SecurityInformation, byte[] SecurityDescriptor);

    [DllImport("kernelbase.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern bool DeriveCapabilitySidsFromName(string CapName, out IntPtr CapabilityGroupSids, out uint CapabilityGroupSidCount, out IntPtr CapabilitySids, out uint CapabilitySidCount);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern bool ConvertSidToStringSidW(IntPtr Sid, out IntPtr StringSid);

    [DllImport("kernel32.dll")]
    static extern IntPtr LocalFree(IntPtr hMem);

    public static string GetToolchainCapabilitySid() {
        IntPtr groupSids = IntPtr.Zero, capSids = IntPtr.Zero;
        uint groupCount = 0, capCount = 0;
        if (!DeriveCapabilitySidsFromName("oxideswarm.toolchain", out groupSids, out groupCount, out capSids, out capCount)) {
            throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        try {
            if (capCount == 0 || capSids == IntPtr.Zero) throw new InvalidOperationException("DeriveCapabilitySidsFromName returned 0 SIDs");
            IntPtr firstSid = Marshal.ReadIntPtr(capSids);
            IntPtr strPtr = IntPtr.Zero;
            if (!ConvertSidToStringSidW(firstSid, out strPtr) || strPtr == IntPtr.Zero) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            try { return Marshal.PtrToStringUni(strPtr); } finally { LocalFree(strPtr); }
        } finally {
            if (groupSids != IntPtr.Zero) {
                for (int i = 0; i < (int)groupCount; i++) { IntPtr s = Marshal.ReadIntPtr(groupSids, i * IntPtr.Size); if (s != IntPtr.Zero) LocalFree(s); }
                LocalFree(groupSids);
            }
            if (capSids != IntPtr.Zero) {
                for (int i = 0; i < (int)capCount; i++) { IntPtr s = Marshal.ReadIntPtr(capSids, i * IntPtr.Size); if (s != IntPtr.Zero) LocalFree(s); }
                LocalFree(capSids);
            }
        }
    }

    static bool DaclAlreadyHasCapabilityRx(DiscretionaryAcl dacl, SecurityIdentifier capSid, int rxMask, bool requireInherit) {
        if (dacl == null) return false;
        foreach (GenericAce ace in dacl) {
            CommonAce cace = ace as CommonAce;
            if (cace != null && cace.AceType == AceType.AccessAllowed && cace.SecurityIdentifier.Equals(capSid)) {
                if ((cace.AccessMask & rxMask) == rxMask) {
                    if (!requireInherit) return true;
                    AceFlags need = AceFlags.ContainerInherit | AceFlags.ObjectInherit;
                    if ((cace.AceFlags & need) == need) return true;
                }
            }
        }
        return false;
    }

    public static string GrantToolchainRx(string path, string capSidStr, bool isDir) {
        const int RX_MASK = 0x001200A9; // FILE_GENERIC_READ | FILE_GENERIC_EXECUTE | SYNCHRONIZE
        IntPtr h = CreateFileW(path, READ_CONTROL | WRITE_DAC, FILE_SHARE_ALL, IntPtr.Zero, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, IntPtr.Zero);
        if (h == new IntPtr(-1)) throw new Win32Exception(Marshal.GetLastWin32Error());
        try {
            BY_HANDLE_FILE_INFORMATION info;
            if (!GetFileInformationByHandle(h, out info)) throw new Win32Exception(Marshal.GetLastWin32Error());
            if ((info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) return "link";
            uint needed = 0;
            GetKernelObjectSecurity(h, DACL_SECURITY_INFORMATION, null, 0, out needed);
            if (needed == 0) throw new Win32Exception(Marshal.GetLastWin32Error());
            byte[] sdBuf = new byte[needed];
            if (!GetKernelObjectSecurity(h, DACL_SECURITY_INFORMATION, sdBuf, needed, out needed)) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            RawSecurityDescriptor raw = new RawSecurityDescriptor(sdBuf, 0);
            CommonSecurityDescriptor csd = new CommonSecurityDescriptor(isDir, false, raw);
            SecurityIdentifier capSid = new SecurityIdentifier(capSidStr);
            if (DaclAlreadyHasCapabilityRx(csd.DiscretionaryAcl, capSid, RX_MASK, isDir)) {
                return null;
            }
            InheritanceFlags inh = isDir ? (InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit) : InheritanceFlags.None;
            csd.DiscretionaryAcl.AddAccess(AccessControlType.Allow, capSid, RX_MASK, inh, PropagationFlags.None);
            byte[] outBuf = new byte[csd.BinaryLength];
            csd.GetBinaryForm(outBuf, 0);
            if (!SetKernelObjectSecurity(h, DACL_SECURITY_INFORMATION, outBuf)) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            return null;
        } finally {
            CloseHandle(h);
        }
    }

    public static void GrantToolchainRxRecursive(string rootDir, string capSidStr) {
        string rErr = GrantToolchainRx(rootDir, capSidStr, true);
        if (rErr == "link") throw new InvalidOperationException(rootDir + " is a link");
        var stack = new System.Collections.Generic.Stack<string>();
        stack.Push(rootDir);
        while (stack.Count > 0) {
            string cur = stack.Pop();
            foreach (string subDir in System.IO.Directory.GetDirectories(cur)) {
                var di = new System.IO.DirectoryInfo(subDir);
                if ((di.Attributes & System.IO.FileAttributes.ReparsePoint) != 0) continue;
                GrantToolchainRx(subDir, capSidStr, true);
                stack.Push(subDir);
            }
            foreach (string file in System.IO.Directory.GetFiles(cur)) {
                var fi = new System.IO.FileInfo(file);
                if ((fi.Attributes & System.IO.FileAttributes.ReparsePoint) != 0) continue;
                GrantToolchainRx(file, capSidStr, false);
            }
        }
    }
}
'@
    }
    $toolchainCapSid = [OxideSwarmToolchainAcl]::GetToolchainCapabilitySid()
}

if ($selectedToolEntries.Count -gt 0) {
    New-Item -ItemType Directory -Force -Path $ToolsDir | Out-Null
    # Grant (OI)(CI)(RX) on tools\ before creating tool trees so newly extracted files inherit it.
    if ($Isolation -eq 'fs') {
        $existingGrants = if (Test-Path -LiteralPath $GrantLog) { @(Get-Content -LiteralPath $GrantLog) } else { @() }
        if ($existingGrants -notcontains $ToolsDir) {
            Add-Content -LiteralPath $GrantLog -Value $ToolsDir -Encoding ASCII
        }
        $gErr = [OxideSwarmToolchainAcl]::GrantToolchainRx($ToolsDir, $toolchainCapSid, $true)
        if ($gErr) { Fail "could not grant isolation ACL on ${ToolsDir}: $gErr" }
    }
    $existingToolSubdirs = @(Get-ChildItem -LiteralPath $ToolsDir -Directory -Force -ErrorAction SilentlyContinue | Where-Object { $_.Name -match '^(python|rust|mingw|git)-' })

    foreach ($tn in $kitToolset) {
        $entries = @($selectedToolEntries | Where-Object { $_.Tool -eq $tn })
        $ver = $entries[0].Version
        if (-not $ver -or $ver.Contains('..') -or $ver -notmatch '^[A-Za-z0-9.+_-]+$') {
            Fail "invalid tool version '$ver' for $tn"
        }
        $targetDir = Join-Path $ToolsDir ("$tn-$ver")
        $stamp = (@($entries | ForEach-Object { $_.Sha256 }) -join '+')
        $stampFile = Join-Path $targetDir '.tool-sha256'
        $alreadyCurrent = (Test-Path -LiteralPath $stampFile) -and ((Get-Content -Raw -LiteralPath $stampFile).Trim() -eq $stamp)
        if (-not $alreadyCurrent) {
            if (Test-Path -LiteralPath $targetDir) { Remove-TreeNoFollow $targetDir }
            $stageDir = Join-Path $ToolsDir (".stage-$tn-" + [guid]::NewGuid().ToString('N'))
            New-Item -ItemType Directory -Force -Path $stageDir | Out-Null
            try {
                if ($tn -eq 'rust') {
                    $rustOut = Join-Path $ToolsDir ("rust-$ver")
                    New-Item -ItemType Directory -Force -Path $rustOut | Out-Null
                    foreach ($re in $entries) {
                        $compStage = Join-Path $stageDir $re.Component
                        New-Item -ItemType Directory -Force -Path $compStage | Out-Null
                        Expand-ToolArchive (Join-Path $Kit ("tools\" + $re.FileName)) $compStage
                        $pkgDirs = @(Get-ChildItem -LiteralPath $compStage -Directory -Force)
                        $pkgRoot = if ($pkgDirs.Count -eq 1 -and -not (Test-Path -LiteralPath (Join-Path $compStage 'bin'))) { $pkgDirs[0].FullName } else { $compStage }
                        Merge-RustComponentPackage $pkgRoot $rustOut
                    }
                    New-Item -ItemType Directory -Force -Path $PrivateCargoHome | Out-Null
                } else {
                    $arcPath = Join-Path $Kit ("tools\" + $entries[0].FileName)
                    Expand-ToolArchive $arcPath $stageDir
                    if ($tn -eq 'mingw') {
                        $subs = @(Get-ChildItem -LiteralPath $stageDir -Directory -Force)
                        $unwrapped = if ($subs.Count -eq 1 -and -not (Test-Path -LiteralPath (Join-Path $stageDir 'bin')) -and (Test-Path -LiteralPath (Join-Path $subs[0].FullName 'bin'))) { $subs[0].FullName } else { $stageDir }
                        if ($unwrapped -eq $stageDir) {
                            Move-Item -LiteralPath $stageDir -Destination $targetDir -Force
                        } else {
                            Move-Item -LiteralPath $unwrapped -Destination $targetDir -Force
                            Remove-TreeNoFollow $stageDir
                        }
                    } else {
                        Move-Item -LiteralPath $stageDir -Destination $targetDir -Force
                    }
                }
            } catch {
                Remove-TreeNoFollow $stageDir
                Fail "failed to extract $tn tool archive: $_"
            } finally {
                if (Test-Path -LiteralPath $stageDir) { Remove-TreeNoFollow $stageDir }
            }
            Set-Content -LiteralPath $stampFile -Value $stamp -Encoding ASCII
        }
        $installedToolDirs[$tn] = $targetDir
        $installedToolNames += $tn
        Say "  tool:    $tn $ver -> $targetDir"
    }
    Copy-Item -LiteralPath $KitNoticeFile -Destination (Join-Path $ToolsDir 'NOTICE.txt') -Force
    if ($Isolation -eq 'fs') {
        [OxideSwarmToolchainAcl]::GrantToolchainRxRecursive($ToolsDir, $toolchainCapSid)
    }
    foreach ($ed in $existingToolSubdirs) {
        $keepDir = $false
        foreach ($kv in $installedToolDirs.GetEnumerator()) {
            if ([string]::Equals($ed.FullName.TrimEnd('\'), $kv.Value.TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase)) {
                $keepDir = $true
                break
            }
        }
        if (-not $keepDir) { $oldToolDirsToClean += $ed.FullName }
    }
} elseif ($Update -and (Test-Path -LiteralPath $ToolsDir)) {
    foreach ($tn in @('python', 'rust', 'mingw', 'git')) {
        $existingMatch = Get-ChildItem -LiteralPath $ToolsDir -Directory -Force -Filter "$tn-*" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($existingMatch) {
            $installedToolDirs[$tn] = $existingMatch.FullName
            $installedToolNames += $tn
        }
    }
}

# Every command below keeps its state in $State and runs from $InstallRoot.
$env:OXIDESWARM_HOME = $State
$env:RUSTY_GRID_ACL_GRANT_LOG = $GrantLog
$env:RUSTY_GRID_SANDBOX_BASE_DIR = $SandboxBase
Set-Location $InstallRoot

# 4. Join: the token (v0) or encrypted invite file (v1) is consumed directly. An update keeps
#    the existing membership.
if (-not $Update) {
    Write-Host ""
    Write-Host "Joining the cluster ..."
    if (-not $IsV1Kit) { Get-Content -Raw $TokenFile | & $Exe join --no-doctor }
    else { & $Exe join --invite-file $InviteFile --no-doctor }
    if ($LASTEXITCODE -ne 0) { Fail "join failed (see the message above); nothing was started" }
}
$pointerParent = Split-Path -Parent $PointerKey
$pointerFull = if ($pointer) { [IO.Path]::GetFullPath($pointer).TrimEnd('\') } else { $null }
if (-not ($pointerFull -and -not [string]::Equals($pointerFull, $InstallRoot, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path (Join-Path $pointerFull 'bin\oxideswarm.exe')))) {
    foreach ($k in @($pointerParent, $PointerKey)) { if ($k -and -not (Test-Path $k)) { New-Item -Path $k | Out-Null } }
    Set-ItemProperty -Path $PointerKey -Name InstallRoot -Value $InstallRoot
    $script:CreatedPointer = $freshInstall
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
# The name goes into the worker's command line and the launcher, so only safe characters pass.
if ($Name -notmatch '^[A-Za-z0-9_.-]{1,64}$') { Fail "the name '$Name' may only hold letters, digits, '.', '_' and '-' (at most 64)" }
if ($KitHasWorker) {
# A stable worker id for this machine and name in this cluster, so that a reinstall shows up as the
# same worker instead of a new one beside a disconnected old entry. It is a SHA-256 over the
# machine's own id (MachineGuid), the name and the cluster secret (or master_key on v1 invite kits):
# the machine id never leaves the machine, and the same machine gets unrelated ids in different
# clusters. An existing id file is kept. OXIDESWARM_USB_MACHINE_ID replaces the machine id (tests).
$idFile = Join-Path $State "worker_id.$Name"
if (-not (Test-Path $idFile) -or (Get-Item $idFile).Length -eq 0) {
    $machineId = $env:OXIDESWARM_USB_MACHINE_ID
    if (-not $machineId) { try { $machineId = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Cryptography' -Name MachineGuid -ErrorAction Stop).MachineGuid } catch { $machineId = $null } }
    if (-not $machineId) { try { $machineId = (Get-CimInstance Win32_ComputerSystemProduct -ErrorAction Stop).UUID } catch { $machineId = $null } }
    if ($machineId) {
        $secretFile = Join-Path $State 'cluster.secret'
        if ((Test-Path -LiteralPath $secretFile) -and (Get-Item -LiteralPath $secretFile).Length -gt 0) {
            $secretBytes = [IO.File]::ReadAllBytes($secretFile)
        } else {
            $masterKeySeed = $null
            $masterKeyFile = Join-Path $State 'master_key'
            if ((Test-Path -LiteralPath $masterKeyFile) -and (Get-Item -LiteralPath $masterKeyFile).Length -gt 0) {
                $masterKeySeed = (Get-Content -Raw -LiteralPath $masterKeyFile).Trim()
            } elseif (Test-Path -LiteralPath $cfg) {
                foreach ($line in Get-Content -LiteralPath $cfg) {
                    if ($line -match '^\s*master_key\s*=\s*(.+?)\s*(?:#.*)?$') {
                        $v = $Matches[1].Trim()
                        if (($v.StartsWith("'") -and $v.EndsWith("'")) -or ($v.StartsWith('"') -and $v.EndsWith('"'))) { $v = $v.Substring(1, $v.Length - 2) }
                        $masterKeySeed = $v
                        break
                    }
                }
            }
            $secretBytes = if ($masterKeySeed) { [Text.Encoding]::UTF8.GetBytes($masterKeySeed + "`n") } else { [byte[]]@() }
        }
        $seed = [Text.Encoding]::UTF8.GetBytes("oxideswarm-usb-worker-id-v1`n$machineId`n$Name`n") + $secretBytes
        $h = [Security.Cryptography.SHA256]::Create().ComputeHash([byte[]]$seed)
        $h[6] = ($h[6] -band 0x0f) -bor 0x80   # UUID version 8 (vendor-specific)
        $h[8] = ($h[8] -band 0x3f) -bor 0x80   # RFC 9562 variant
        $hex = -join ($h[0..15] | ForEach-Object { $_.ToString('x2') })
        $uuid = '{0}-{1}-{2}-{3}-{4}' -f $hex.Substring(0, 8), $hex.Substring(8, 4), $hex.Substring(12, 4), $hex.Substring(16, 4), $hex.Substring(20, 12)
        [IO.File]::WriteAllText($idFile, $uuid + "`n")
    }
}
# Task slots (USB-4): the logical processors of the performance cores, at most half of all
# processors and at least 1, so the machine stays responsive for its owner. A laptop chip with few
# performance cores slows down far more than linearly when every efficient core is busy too. When
# the core types cannot be read, a machine with a battery gets a quarter of its processors.
# OXIDESWARM_USB_CPU_INFO="total=N perf=N|- battery=0|1" replaces the probe (tests).
function Get-CpuInfo {
    $info = @{ total = [Environment]::ProcessorCount; perf = 0; battery = $false }
    if ($env:OXIDESWARM_USB_CPU_INFO) {
        foreach ($kv in ($env:OXIDESWARM_USB_CPU_INFO -split '\s+')) {
            if ($kv -match '^total=(\d+)$') { $info.total = [int]$Matches[1] }
            elseif ($kv -match '^perf=(\d+)$') { $info.perf = [int]$Matches[1] }
            elseif ($kv -match '^battery=1$') { $info.battery = $true }
        }
        return $info
    }
    try {
        if (-not ('OxideSwarmCpuSets' -as [type])) {
            Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class OxideSwarmCpuSets {
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetSystemCpuSetInformation(IntPtr information, uint bufferLength, out uint returnedLength, IntPtr process, uint flags);
    // Logical processors in the highest efficiency class (the performance cores), or -1.
    public static int PerformanceThreads() {
        uint needed;
        GetSystemCpuSetInformation(IntPtr.Zero, 0, out needed, IntPtr.Zero, 0);
        if (needed == 0) return -1;
        IntPtr buffer = Marshal.AllocHGlobal((int)needed);
        try {
            uint got;
            if (!GetSystemCpuSetInformation(buffer, needed, out got, IntPtr.Zero, 0)) return -1;
            int best = -1, count = 0, offset = 0;
            while (offset + 19 <= (int)got) {
                int size = Marshal.ReadInt32(buffer, offset);
                int type = Marshal.ReadInt32(buffer, offset + 4);
                if (size <= 0) break;
                if (type == 0) {
                    // SYSTEM_CPU_SET_INFORMATION.CpuSet.EfficiencyClass
                    int cls = Marshal.ReadByte(buffer, offset + 18);
                    if (cls > best) { best = cls; count = 1; } else if (cls == best) { count++; }
                }
                offset += size;
            }
            return best < 0 ? -1 : count;
        } finally {
            Marshal.FreeHGlobal(buffer);
        }
    }
}
'@
        }
        $perf = [OxideSwarmCpuSets]::PerformanceThreads()
        if ($perf -ge 1) { $info.perf = $perf }
    } catch { }
    try { if (Get-CimInstance Win32_Battery -ErrorAction Stop) { $info.battery = $true } } catch { }
    return $info
}
$cpu = Get-CpuInfo
$cpuTotal = [Math]::Max(1, [int]$cpu.total)
$half = [Math]::Max(1, [int][Math]::Floor($cpuTotal / 2))
if ($slotsSet -ge 1) {
    $slots = $slotsSet; $slotsWhy = 'set with -Slots'
} elseif ($cpu.perf -ge 1) {
    $slots = [Math]::Min([int]$cpu.perf, $half); $slotsWhy = "performance cores, at most half of $cpuTotal processors"
} elseif ($cpu.battery) {
    $slots = [Math]::Max(1, [int][Math]::Floor($cpuTotal / 4)); $slotsWhy = "a quarter of $cpuTotal processors on a machine with a battery"
} else {
    $slots = $half; $slotsWhy = "half of $cpuTotal processors"
}
Write-Host "  slots:   $slots ($slotsWhy; change it with -Slots N)"
$hasPrivateCargo = ($installedToolDirs.ContainsKey('rust') -and (Test-Path -LiteralPath (Join-Path $installedToolDirs['rust'] 'bin\cargo.exe')))
$workerArgs = "worker --name $Name --no-portal --max-concurrency $slots --isolation $Isolation"
# Release 0.1.1 predates FEAT-3d (--workspaces under Windows --isolation fs), so omit --workspaces on 0.1.0/0.1.1 when -Isolation fs is used.
$supportsIsolatedWorkspaces = ($Isolation -eq 'off') -or ($version -notmatch '\b0\.1\.[01](\b|-)')
if ($useWorkspaces -and $supportsIsolatedWorkspaces -and ($hasPrivateCargo -or (Get-Command cargo -ErrorAction SilentlyContinue))) {
    # Always pass an explicit workspace root inside the install folder so the USB worker never shares
    # %LOCALAPPDATA%\oxideswarm\workspaces (the default workspace root used by non-USB workers).
    $WorkspaceRoot = if ([string]::Equals($InstallRoot, $LegacyDefaultRoot, [StringComparison]::OrdinalIgnoreCase)) { Join-Path $InstallRoot 'usb-kit\workspaces' } else { Join-Path $InstallRoot 'workspaces' }
    $WorkspaceMarker = Join-Path $WorkspaceRoot '.oxideswarm-usb-workspaces'
    if (-not (Test-Path -LiteralPath $WorkspaceRoot)) {
        New-Item -ItemType Directory -Force -Path $WorkspaceRoot | Out-Null
        Set-Content -LiteralPath $WorkspaceMarker -Value 'Created by an OxideSwarm USB kit install.' -Encoding ASCII
    } elseif (-not (Test-Path -LiteralPath $WorkspaceMarker)) {
        $existingWsEntries = @(Get-ChildItem -LiteralPath $WorkspaceRoot -Force -ErrorAction SilentlyContinue)
        $hadPrevWsRoot = $false
        if ($Update) {
            $prevVbs = Join-Path $InstallRoot 'start-worker.vbs'
            if ((Test-Path -LiteralPath $prevVbs) -and ((Get-Content -Raw -LiteralPath $prevVbs) -match '--workspace-root')) {
                $hadPrevWsRoot = $true
            }
        }
        if ($existingWsEntries.Count -eq 0 -or $hadPrevWsRoot) {
            Set-Content -LiteralPath $WorkspaceMarker -Value 'Created by an OxideSwarm USB kit install.' -Encoding ASCII
        }
    }
    $workerArgs += ' --workspaces --workspace-root "' + $WorkspaceRoot + '"'
}
if ($Isolation -eq 'fs' -and $installedToolNames.Count -gt 0 -and (Test-Path -LiteralPath $ToolsDir) -and ($version -notmatch '\b0\.1\.[01](\b|-)')) {
    $workerArgs += ' --isolation-read-dir "' + $ToolsDir + '"'
}
$log = Join-Path $LogDir 'worker.log'

$toolPathList = @()
if ($installedToolDirs.ContainsKey('python')) { $toolPathList += $installedToolDirs['python'] }
if ($installedToolDirs.ContainsKey('rust')) { $toolPathList += (Join-Path $installedToolDirs['rust'] 'bin') }
if ($installedToolDirs.ContainsKey('mingw')) { $toolPathList += (Join-Path $installedToolDirs['mingw'] 'bin') }
if ($installedToolDirs.ContainsKey('git')) { $toolPathList += (Join-Path $installedToolDirs['git'] 'cmd') }
if ($hasPrivateCargo) {
    $CargoHome = $PrivateCargoHome
    $gnuLinker = Join-Path $installedToolDirs['rust'] 'lib\rustlib\x86_64-pc-windows-gnu\bin\self-contained\x86_64-w64-mingw32-gcc.exe'
    $gnuLinkerToml = $gnuLinker.Replace('\', '/')
    if (-not (Test-Path -LiteralPath $PrivateCargoHome)) {
        New-Item -ItemType Directory -Force -Path $PrivateCargoHome | Out-Null
    }
    # Write cargo-home\config.toml too so AppContainer tasks (--isolation fs, which filter unknown
    # CARGO_* env vars) still bind Rust's self-contained GNU linker cleanly.
    Set-Content -LiteralPath (Join-Path $PrivateCargoHome 'config.toml') -Encoding ASCII -Value @(
        '# Generated by OxideSwarm USB installer (design 0110)',
        '[target.x86_64-pc-windows-gnu]',
        ('linker = "' + $gnuLinkerToml + '"')
    )
}

# A small launcher that starts the worker hidden with this install's state folder. The HKCU Run
# entry uses it at every logon.
$vbs = Join-Path $InstallRoot 'start-worker.vbs'
$cmdLine = 'cmd /c ""' + $Exe + '" ' + $workerArgs + ' >> "' + $log + '" 2>&1"'
$vbsLines = New-Object 'System.Collections.Generic.List[string]'
$vbsLines.Add('Set sh = CreateObject("WScript.Shell")')
$vbsLines.Add('sh.Environment("Process")("OXIDESWARM_HOME") = "' + $State + '"')
$vbsLines.Add('sh.Environment("Process")("HOME") = "' + $State + '"')
if ($hasPrivateCargo) {
    # No rustup in private kit mode: ensure RUSTUP_HOME stays unset and bind Rust's self-contained
    # GNU linker so w64devkit ld is not selected (missing libgcc_eh).
    $vbsLines.Add('On Error Resume Next')
    $vbsLines.Add('sh.Environment("Process").Remove("RUSTUP_HOME")')
    $vbsLines.Add('sh.Environment("Process").Remove("CARGO_TARGET_DIR")')
    $vbsLines.Add('On Error GoTo 0')
    $vbsLines.Add('sh.Environment("Process")("CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER") = "' + $gnuLinker + '"')
} else {
    $vbsLines.Add('sh.Environment("Process")("RUSTUP_HOME") = "' + $RustupHome + '"')
}
$vbsLines.Add('sh.Environment("Process")("CARGO_HOME") = "' + $CargoHome + '"')
$vbsLines.Add('sh.Environment("Process")("RUSTY_GRID_ACL_GRANT_LOG") = "' + $GrantLog + '"')
$vbsLines.Add('sh.Environment("Process")("RUSTY_GRID_SANDBOX_BASE_DIR") = "' + $SandboxBase + '"')
if ($toolPathList.Count -gt 0) {
    $vbsLines.Add('sh.Environment("Process")("PATH") = "' + ($toolPathList -join ';') + ';" & sh.Environment("Process")("PATH")')
}
$vbsLines.Add('sh.CurrentDirectory = "' + $InstallRoot + '"')
$vbsLines.Add('sh.Run "' + $cmdLine.Replace('"', '""') + '", 0, False')
$vbsLines | Set-Content -Path $vbs -Encoding ASCII

# A failed start (BUG-31): the end of the worker's log under a header, then the error. The rollback
# keeps <install folder>\logs ($script:KeepLogs), so the file the error names still exists.
function Fail-WorkerStart($what) {
    Write-Host ""
    if ((Test-Path -LiteralPath $log) -and (Get-Item -LiteralPath $log).Length -gt 0) {
        Write-Host "----- last 30 lines of $log -----"
        try { Get-Content -LiteralPath $log -Tail 30 -ErrorAction Stop | ForEach-Object { Write-Host "  $_" } } catch { Write-Host "  (it could not be read: $($_.Exception.Message))" }
        Write-Host "----- end of $log -----"
    } else {
        Write-Host "(the worker wrote nothing to $log)"
    }
    $kept = if (Test-Path -LiteralPath $log) { $log } else { $script:InstallLogPath }
    FailAfterStart "$what; its log is kept in $kept"
}
function Test-WorkerStopped {
    if ($workerProc) { try { return [bool]$workerProc.HasExited } catch { } }
    return -not (Get-Process -Id $workerPid -ErrorAction SilentlyContinue)
}
function Get-WorkerExitCode {
    if ($workerProc) { try { if ($workerProc.HasExited) { return "$($workerProc.ExitCode)" } } catch { } }
    return 'unknown'
}

# 6. Start it now through the same hidden launcher, so that closing this window does not stop it.
Write-Host ""
Write-Host "Starting worker '$Name' ..."
$script:KeepLogs = $true
$startClock = [Diagnostics.Stopwatch]::StartNew()
Start-Process -FilePath 'wscript.exe' -ArgumentList ('"' + $vbs + '"') | Out-Null
$workerPid = $null
$workerProc = $null
for ($i = 0; $i -lt 10 -and -not $workerPid; $i++) {
    Start-Sleep -Seconds 1
    $workerPid = Get-CimInstance Win32_Process |
        Where-Object { $_.ExecutablePath -eq $Exe -and $_.CommandLine -match '(?:oxideswarm|rusty-grid)(?:\.exe)?["'']?\s+(worker)(\s|$)' } |
        Select-Object -First 1 -ExpandProperty ProcessId
}
if (-not $workerPid) { Fail-WorkerStart "the worker did not start within 10 s" }
$workerPid | Set-Content $pidFile
# A handle held from now on lets the installer read the worker's exit code after it ends.
try { $workerProc = Get-Process -Id $workerPid -ErrorAction Stop; $null = $workerProc.Handle } catch { $workerProc = $null }

# 7. Wait until the master lists it, and check every second that the worker still runs (BUG-31: one
# check 3 s after the start missed a worker that stopped after about 6 s). The listing runs in a
# runspace of its own, so a slow answer does not hold up that check. The checks run without log
# lines: release 0.1.x writes its informational logs to standard output, which would break the JSON
# parsing below. The worker was started above, so it keeps its own logging.
$env:RUST_LOG = 'off'
$listScript = {
    param($exe, $checkArgs)
    $env:RUST_LOG = 'off'
    # The JSON listing carries the real state; the plain listing also shows disconnected workers.
    (& $exe workers --json @checkArgs 2>$null | Out-String)
}
$lister = [PowerShell]::Create()
$connected = $false
$connectedWorkerTags = @()
$stoppedAfter = $null
$waited = 0
$pending = $null
while (-not $connected -and $null -eq $stoppedAfter -and $waited -lt 30) {
    $lister.Commands.Clear()
    [void]$lister.AddScript($listScript.ToString()).AddArgument($Exe).AddArgument($checkArgs)
    $pending = $lister.BeginInvoke()
    while ($true) {
        Start-Sleep -Seconds 1
        $waited++
        if (Test-WorkerStopped) { $stoppedAfter = [int]$startClock.Elapsed.TotalSeconds; break }
        if ($pending.IsCompleted -or $waited -ge 30) { break }
    }
    if (-not $pending.IsCompleted) { try { $lister.Stop() } catch { }; break }
    $list = $null
    try { $list = (@($lister.EndInvoke($pending)) -join "`n") | ConvertFrom-Json } catch { $list = $null }
    if ($null -eq $stoppedAfter) {
        $matchedWorker = @($list | Where-Object { $_.name -eq $Name -and $_.tags -contains 'state:connected' }) | Select-Object -First 1
        if ($matchedWorker) {
            $connected = $true
            if ($matchedWorker.tags) { $connectedWorkerTags = @($matchedWorker.tags | ForEach-Object { "$_" }) }
        }
    }
}
if ($pending -and $pending.IsCompleted) { try { $lister.Dispose() } catch { } }
if ($null -ne $stoppedAfter) { Fail-WorkerStart "the worker stopped $stoppedAfter s after it started (exit code $(Get-WorkerExitCode))" }
if (-not $connected) { Fail-WorkerStart "the worker ran for 30 s, but the master did not list it" }
Say "  connected (isolation: $Isolation)"
if ($Update -and $oldToolDirsToClean.Count -gt 0) {
    foreach ($oldTd in $oldToolDirsToClean) {
        if (Test-Path -LiteralPath $oldTd) {
            Remove-TreeNoFollow $oldTd
            Say "  tools:   removed old folder $oldTd"
        }
    }
}
}
# What an update keeps: the name, the build, the workspace choice, roles, tools and isolation.
$builtWithGpu = if ($KitHasWorker -and $GpuArchive -and $Archive.FullName -eq $GpuArchive.FullName) { '1' } else { '0' }
$withWorkspaces = if ($useWorkspaces) { '1' } else { '0' }
$withTools = if ($useTools) { '1' } else { '0' }
$withWorkerRole = if ($KitHasWorker) { '1' } else { '0' }
$withClientRole = if ($KitHasClient) { '1' } else { '0' }
$recordedSlots = if ($KitHasWorker -and $slotsSet -ge 1) { "$slotsSet" } else { '' }
Set-Content -Path (Join-Path $InstallRoot 'kit-install.txt') -Encoding ASCII -Value @(
    "name=$Name",
    "gpu=$builtWithGpu",
    "workspaces=$withWorkspaces",
    "tools=$withTools",
    "toolset=$($installedToolNames -join ',')",
    "isolation=$Isolation",
    "slots=$recordedSlots",
    "worker=$withWorkerRole",
    "client=$withClientRole"
)

# 8. Start at logon (current user only, no administrator rights).
if ($KitHasWorker) {
    if (-not $NoAutostart) {
        Set-ItemProperty -Path $RunKey -Name $runValueName -Value ('wscript.exe "' + $vbs + '"')
        $script:CreatedRunEntry = $freshInstall
        Say "  autostart: HKCU Run entry $runValueName"
    } else {
        Say "  autostart: skipped (-NoAutostart)"
    }
}

$script:RollbackArmed = $false

if ($Update) {
    Write-Host ""
    if ($KitHasWorker) {
        Write-Host "Updated. Worker '$Name' runs $(& $Exe --version) from $InstallRoot."
        Write-LogRecord "Updated. Worker '$Name' runs from $InstallRoot."
    } else {
        Write-Host "Updated. Worker '$Name' (client mode) runs $(& $Exe --version) from $InstallRoot."
        Write-LogRecord "Updated. Client '$Name' runs from $InstallRoot."
    }
    exit 0
}

# 9. Check the cluster (and each installed developer tool per design 0110 D5).
if ($KitHasClient) {
    $env:RUST_LOG = 'off'
    Write-Host ""
    Write-Host "Checking the cluster (oxideswarm doctor) ..."
    & $Exe doctor @checkArgs
    $doctor = if ($LASTEXITCODE -eq 0) { "passed" } else { "reported a problem (see above)" }
    if ($KitHasWorker -and $installedToolNames.Count -gt 0) {
        Write-Host ""
        Write-Host "Checking installed developer tools ..."
        $checkTmp = Join-Path ([IO.Path]::GetTempPath()) ("oxideswarm-toolcheck-" + [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Force -Path $checkTmp | Out-Null
        function Invoke-ToolCheckCmd([scriptblock]$action) {
            $prevEap = $ErrorActionPreference
            $ErrorActionPreference = 'Continue'
            try {
                $global:LASTEXITCODE = 0
                $txt = (& $action 2>&1 | Out-String).Trim()
                return @{ code = $LASTEXITCODE; output = $txt }
            } finally {
                $ErrorActionPreference = $prevEap
            }
        }
        try {
            if ($installedToolDirs.ContainsKey('python')) {
                $pyReqTool = if ($connectedWorkerTags -contains 'tool:python') { @('--require-tool', 'python') } else { @() }
                if ($pyReqTool.Count -eq 0) {
                    Say "  note:          worker does not advertise tool:python yet; probing Python via --worker $Name"
                }
                $rPy = Invoke-ToolCheckCmd { & $Exe submit --worker $Name @pyReqTool --wait --timeout-secs 30 @checkArgs -- python -c "import sys; print(sys.version)" }
                if ($rPy.code -eq 0) {
                    Say "  check python:  OK ($((($rPy.output -split "`r?`n") | Select-Object -Last 1).Trim()))"
                } else {
                    Say "  check python:  FAILED ($($rPy.output))"
                }
            }
            if ($installedToolDirs.ContainsKey('rust')) {
                $rRc = Invoke-ToolCheckCmd { & $Exe submit --worker $Name --require-tool rustc --wait --timeout-secs 30 @checkArgs -- rustc --version }
                if ($rRc.code -eq 0) {
                    Say "  check rustc:   OK ($((($rRc.output -split "`r?`n") | Select-Object -Last 1).Trim()))"
                } else {
                    Say "  check rustc:   FAILED ($($rRc.output))"
                }
                $helloSrc = Join-Path $Kit 'tools\check\hello.rs'
                $helloExe = Join-Path $checkTmp 'hello.exe'
                if (Test-Path -LiteralPath $helloSrc) {
                    $rComp = Invoke-ToolCheckCmd { & $Exe submit --worker $Name --require-tool rustc --type compile --source $helloSrc --output-dir $checkTmp --output-asset $helloExe --timeout-secs 60 @checkArgs -- --crate-type bin -C "linker=$gnuLinker" }
                    if ($rComp.code -eq 0 -and (Test-Path -LiteralPath $helloExe)) {
                        $rRunHello = Invoke-ToolCheckCmd { & $helloExe }
                        if ($rRunHello.code -eq 0 -and $rRunHello.output -match 'oxideswarm-tool-check-ok') {
                            Say "  check compile: OK ($($rRunHello.output))"
                        } else {
                            Say "  check compile: FAILED running compiled binary ($($rRunHello.output))"
                        }
                    } else {
                        Say "  check compile: FAILED ($($rComp.output))"
                    }
                }
            }
            if ($installedToolDirs.ContainsKey('rust') -and $installedToolDirs.ContainsKey('mingw')) {
                $ccCheckDir = Join-Path $Kit 'tools\check\cc-check'
                if (Test-Path -LiteralPath $ccCheckDir) {
                    $rCc = Invoke-ToolCheckCmd { & $Exe submit --worker $Name --require-tool cargo --input "${ccCheckDir}:cc-check" --wait --timeout-secs 120 @checkArgs -- cmd /c "cargo build --release --offline --manifest-path cc-check\Cargo.toml --target-dir cc-check\target && cc-check\target\release\cc-check.exe" }
                    if ($rCc.code -eq 0 -and $rCc.output -match 'add_c\(40, 2\) = 42') {
                        Say "  check mingw:   OK (cc-check --release: add_c(40, 2) = 42)"
                    } else {
                        Say "  check mingw:   FAILED ($($rCc.output))"
                    }
                }
            }
            if ($installedToolDirs.ContainsKey('git')) {
                $rGit = Invoke-ToolCheckCmd { & $Exe submit --worker $Name --require-tool git --wait --timeout-secs 30 @checkArgs -- git --version }
                if ($rGit.code -eq 0) {
                    Say "  check git:     OK ($((($rGit.output -split "`r?`n") | Select-Object -Last 1).Trim()))"
                } else {
                    Say "  check git:     FAILED ($($rGit.output))"
                }
            }
        } finally {
            Remove-Item -LiteralPath $checkTmp -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host ""
    if ($KitHasWorker) {
        Say "Done. Worker '$Name' is part of the cluster; the doctor check $doctor."
        Say "Files: $InstallRoot   Log: $log"
    } else {
        Say "Done. Machine '$Name' is enrolled as a client; the doctor check $doctor."
        Say "Files: $InstallRoot   Log: $script:InstallLogPath"
    }
} else {
    Write-Host ""
    Say "Skipping doctor check (worker-only kit; roles do not include 'client')."
    Say "Done. Worker '$Name' is part of the cluster (doctor skipped: worker-only kit)."
    Say "Files: $InstallRoot   Log: $log"
}
Say "To remove it, run 'Uninstall OxideSwarm.cmd' from the stick."
