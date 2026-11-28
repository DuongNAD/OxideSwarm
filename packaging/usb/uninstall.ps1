# OxideSwarm USB uninstaller for Windows (kit format v0 / v1, design 0053 / 0055 / 0110).
# Stops the worker this kit installed, revokes any oxideswarm.toolchain ACL grants recorded in its
# manifest (<install folder>\home\windows_acl_grants.txt, including private tools\), removes its
# HKCU Run entry, its folder (binaries, private tools\, cargo-home\, credentials, logs, task
# folders and file cache), and %TEMP%\oxideswarm-usb (the task folders of an install with
# -Isolation fs in %LOCALAPPDATA%, and of installs before 2026-11-18).
# Nothing else is touched. Without -InstallRoot it removes the install the installer recorded in HKCU (another
# drive included), else %LOCALAPPDATA%\OxideSwarm-USB (or %LOCALAPPDATA%\OxideSwarm for an older
# install). Pass -KeepKey to retain <install folder>\home\node.key for rejoining with the same identity.
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1 [-InstallRoot DIR] [-KeepKey] [-Yes]
[CmdletBinding()]
param(
    [string]$InstallRoot = "",
    [switch]$KeepKey,
    [switch]$Yes
)
$ErrorActionPreference = 'Stop'
$PointerKey = if ($env:OXIDESWARM_USB_POINTER_KEY) { $env:OXIDESWARM_USB_POINTER_KEY } else { 'HKCU:\Software\OxideSwarm\UsbKit' }
$DefaultRoot = [IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA 'OxideSwarm-USB')).TrimEnd('\')
$LegacyDefaultRoot = [IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA 'OxideSwarm')).TrimEnd('\')
$DefaultWorkerWorkspaceRoot = Join-Path $LegacyDefaultRoot 'workspaces'
$pointer = $null
try { $pointer = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $pointer = $null }
if (-not $InstallRoot) {
    if ($pointer) {
        $InstallRoot = $pointer
    } elseif (-not (Test-Path -LiteralPath (Join-Path $DefaultRoot 'bin\oxideswarm.exe')) -and (Test-Path -LiteralPath (Join-Path $LegacyDefaultRoot 'bin\oxideswarm.exe'))) {
        $InstallRoot = $LegacyDefaultRoot
    } else {
        $InstallRoot = $DefaultRoot
    }
}
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
$Exe = Join-Path $InstallRoot 'bin\oxideswarm.exe'
if (-not (Test-Path -LiteralPath $InstallRoot)) { Write-Host "Nothing to remove: $InstallRoot does not exist."; exit 0 }
if (-not (Test-Path -LiteralPath $Exe)) { Write-Host "ERROR: $InstallRoot does not look like a USB-kit install; not touching it." -ForegroundColor Red; exit 1 }
# Only a folder this kit made is deleted: it carries the installer's marker (installs from before the
# marker existed only in the legacy default folder), it is not a link, and nothing inside is one,
# because Windows PowerShell 5.1 follows junctions while it deletes.
$rootItem = Get-Item -LiteralPath $InstallRoot -Force
if ($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { Write-Host "ERROR: $InstallRoot is a link; not touching it." -ForegroundColor Red; exit 1 }
$isDefault = [string]::Equals($InstallRoot, $LegacyDefaultRoot, [StringComparison]::OrdinalIgnoreCase)
if (-not $isDefault -and -not (Test-Path -LiteralPath (Join-Path $InstallRoot '.oxideswarm-usb-install'))) {
    Write-Host "ERROR: $InstallRoot has no .oxideswarm-usb-install marker; not touching it." -ForegroundColor Red; exit 1
}
$links = @(Get-ChildItem -LiteralPath $InstallRoot -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint } | Select-Object -First 3)
if ($links.Count -gt 0) {
    Write-Host "ERROR: $InstallRoot contains links ($(($links | ForEach-Object { $_.FullName }) -join ', ')); remove them first, then run the uninstaller again." -ForegroundColor Red; exit 1
}
function Get-CommandLineOptionValue($cmdLine, [string]$optName) {
    if (-not $cmdLine) { return $null }
    $esc = [regex]::Escape($optName)
    $pattern = '(?:^|\s)' + $esc + '(?:=|\s+)(?:"([^"]+)"|''([^'']+)''|(\S+))'
    $m = [regex]::Match($cmdLine, $pattern)
    if (-not $m.Success) { return $null }
    for ($i = 1; $i -le 3; $i++) {
        if ($m.Groups[$i].Success) { return $m.Groups[$i].Value }
    }
    return $null
}
function Get-CommandLineWorkspaceRoot($cmdLine, [string]$defaultWsRoot) {
    $explicit = Get-CommandLineOptionValue $cmdLine '--workspace-root'
    if ($explicit) {
        try { return [IO.Path]::GetFullPath($explicit).TrimEnd('\') } catch { return $null }
    }
    if ($defaultWsRoot -and ($cmdLine -match '(?:^|\s)--workspaces(?:\s|$)')) {
        return $defaultWsRoot
    }
    return $null
}
function Get-WorkspaceDirUsers([string]$targetDir, [string]$ownExe, [string]$defaultWsRoot) {
    $users = New-Object 'System.Collections.Generic.List[string]'
    $ownExeFull = [IO.Path]::GetFullPath($ownExe)
    $procs = @()
    try { $procs = @(Get-CimInstance Win32_Process -ErrorAction Stop) } catch { $procs = @() }
    foreach ($p in $procs) {
        if ($p.ExecutablePath) {
            try {
                if ([string]::Equals([IO.Path]::GetFullPath($p.ExecutablePath), $ownExeFull, [StringComparison]::OrdinalIgnoreCase)) { continue }
            } catch { }
        }
        $cmd = $p.CommandLine
        if (-not $cmd) { continue }
        $isOxideBin = ($p.Name -in @('oxideswarm.exe', 'rusty-grid.exe')) -or ($p.ExecutablePath -match '(?:oxideswarm|rusty-grid)(?:\.exe)?$')
        if (-not $isOxideBin -or $cmd -notmatch '(?:^|\s)worker(?:\s|$)') { continue }
        $wsRoot = Get-CommandLineWorkspaceRoot $cmd $defaultWsRoot
        if ($wsRoot -and [string]::Equals($wsRoot, $targetDir, [StringComparison]::OrdinalIgnoreCase)) {
            $wName = Get-CommandLineOptionValue $cmd '--name'
            $label = if ($wName) { "worker '$wName' (PID $($p.ProcessId), $($p.ExecutablePath))" } else { "worker PID $($p.ProcessId) ($($p.ExecutablePath))" }
            $users.Add($label)
        }
    }
    $svcs = @()
    try { $svcs = @(Get-CimInstance Win32_Service -ErrorAction Stop) } catch { $svcs = @() }
    foreach ($s in $svcs) {
        $pathName = $s.PathName
        if (-not $pathName -or $pathName -notmatch '(?:oxideswarm|rusty-grid)(?:\.exe)?' -or $pathName -notmatch '(?:^|\s)worker(?:\s|$)') { continue }
        if ($ownExe -and $pathName.IndexOf($ownExe, [StringComparison]::OrdinalIgnoreCase) -ge 0) { continue }
        $wsRoot = Get-CommandLineWorkspaceRoot $pathName $null
        if ($wsRoot -and [string]::Equals($wsRoot, $targetDir, [StringComparison]::OrdinalIgnoreCase)) {
            $users.Add("Windows service '$($s.Name)' ($pathName)")
        }
    }
    return @($users)
}
# Refuse to delete $InstallRoot if $InstallRoot\workspaces was not created by the USB kit or is
# currently used by another worker (for example a non-USB worker sharing %LOCALAPPDATA%\oxideswarm\workspaces).
$WorkspacesDir = Join-Path $InstallRoot 'workspaces'
if (Test-Path -LiteralPath $WorkspacesDir) {
    $wsMarker = Join-Path $WorkspacesDir '.oxideswarm-usb-workspaces'
    $foreignUsers = @(Get-WorkspaceDirUsers $WorkspacesDir $Exe $DefaultWorkerWorkspaceRoot)
    # An install from a kit older than the .oxideswarm-usb-workspaces marker (BUG-30): its workspaces
    # folder counts as the kit's when the install folder carries the installer's own marker, the
    # folder is a real one directly inside it with no link anywhere inside, no running worker or
    # service uses it, and it is not the default workspace root of a non-USB worker.
    $olderKitWorkspaces = $false
    if (-not (Test-Path -LiteralPath $wsMarker) -and $foreignUsers.Count -eq 0 -and
        -not [string]::Equals($WorkspacesDir, $DefaultWorkerWorkspaceRoot, [StringComparison]::OrdinalIgnoreCase) -and
        (Test-Path -LiteralPath (Join-Path $InstallRoot '.oxideswarm-usb-install'))) {
        $wsItem = Get-Item -LiteralPath $WorkspacesDir -Force
        $wsLinks = @(Get-ChildItem -LiteralPath $WorkspacesDir -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint } | Select-Object -First 1)
        if ($wsItem.PSIsContainer -and -not ($wsItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -and $wsLinks.Count -eq 0) {
            $olderKitWorkspaces = $true
        }
    }
    if ($olderKitWorkspaces) {
        Write-Host "$WorkspacesDir has no .oxideswarm-usb-workspaces marker: an install from an older kit, and no other worker uses it."
    } elseif (-not (Test-Path -LiteralPath $wsMarker -PathType Leaf) -or $foreignUsers.Count -gt 0) {
        if ($foreignUsers.Count -gt 0) {
            Write-Host "ERROR: $WorkspacesDir is used by $($foreignUsers -join '; '); not touching $InstallRoot." -ForegroundColor Red
        } elseif ([string]::Equals($WorkspacesDir, $DefaultWorkerWorkspaceRoot, [StringComparison]::OrdinalIgnoreCase)) {
            Write-Host "ERROR: $WorkspacesDir is the default workspace root of a non-USB OxideSwarm worker (missing .oxideswarm-usb-workspaces marker); not touching $InstallRoot." -ForegroundColor Red
        } else {
            Write-Host "ERROR: $WorkspacesDir was not created by the USB kit (missing .oxideswarm-usb-workspaces marker; used by another OxideSwarm worker or tool); not touching $InstallRoot." -ForegroundColor Red
        }
        exit 1
    }
}
$NodeKeyPath = Join-Path $InstallRoot 'home\node.key'
if (-not $Yes) {
    $ans = Read-Host "Remove the OxideSwarm install in ${InstallRoot}? [y/N]"
    if ($ans -notmatch '^(y|yes)$') { Write-Host "Nothing removed."; exit 0 }
    if (-not $KeepKey -and (Test-Path -LiteralPath $NodeKeyPath)) {
        $keepAns = Read-Host "Keep machine identity key ($NodeKeyPath) so rejoining keeps the same identity? [y/N]"
        if ($keepAns -match '^(y|yes)$') { $KeepKey = $true }
    }
}
# The start at logon belongs to this install only if it runs this install's launcher.
$runKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
$runValueName = if ($env:OXIDESWARM_USB_RUN_VALUE) { $env:OXIDESWARM_USB_RUN_VALUE } else { 'OxideSwarmUsbWorker' }
$run = $null
try { $run = (Get-ItemProperty -Path $runKey -Name $runValueName -ErrorAction Stop).$runValueName } catch { $run = $null }
if ($run -and $run.IndexOf($InstallRoot + '\', [StringComparison]::OrdinalIgnoreCase) -ge 0) {
    Remove-ItemProperty -Path $runKey -Name $runValueName
}
# Stop the worker by the exact path of this install's binary (never by process name).
$pidsToWait = @()
Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq $Exe } | ForEach-Object {
    $pidsToWait += $_.ProcessId
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
for ($i = 0; $i -lt 20; $i++) {
    $stillAlive = @($pidsToWait | Where-Object { Get-Process -Id $_ -ErrorAction SilentlyContinue })
    if ($stillAlive.Count -eq 0) { break }
    Start-Sleep -Milliseconds 250
}
Start-Sleep -Seconds 1

# Revoke any oxideswarm.toolchain ACL grants recorded in this kit's manifest before deleting $InstallRoot.
$Manifest = Join-Path $InstallRoot 'home\windows_acl_grants.txt'
$RevokeScript = $null
foreach ($cand in @(
    (Join-Path $PSScriptRoot 'revoke-isolation-acls.ps1'),
    (Join-Path $InstallRoot 'revoke-isolation-acls.ps1'),
    (Join-Path $PSScriptRoot '..\..\windows\revoke-isolation-acls.ps1')
)) {
    if ($cand -and (Test-Path -LiteralPath $cand)) {
        $RevokeScript = $cand
        break
    }
}
if (Test-Path -LiteralPath $Manifest) {
    if (-not $RevokeScript) {
        Write-Host "ERROR: ACL grant manifest found at $Manifest, but revoke-isolation-acls.ps1 was not found; keeping $InstallRoot and $Manifest." -ForegroundColor Red
        exit 1
    }
    Write-Host "Revoking isolation ACL grants recorded in $Manifest ..."
    $global:LASTEXITCODE = 0
    & $RevokeScript -ManifestPath $Manifest -RemoveManifest
    if ($LASTEXITCODE -ne 0 -or (Test-Path -LiteralPath $Manifest)) {
        Write-Host "ERROR: Isolation ACL revocation did not complete (exit code $LASTEXITCODE); keeping $InstallRoot and $Manifest." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "No isolation ACL grant manifest in $Manifest (nothing to revoke)."
}

if ($KeepKey -and (Test-Path -LiteralPath $NodeKeyPath)) {
    foreach ($child in @(Get-ChildItem -LiteralPath $InstallRoot -Force -ErrorAction SilentlyContinue)) {
        if ($child.Name -in @('home', '.oxideswarm-usb-install')) { continue }
        Remove-Item -LiteralPath $child.FullName -Recurse -Force
    }
    foreach ($homeChild in @(Get-ChildItem -LiteralPath (Join-Path $InstallRoot 'home') -Force -ErrorAction SilentlyContinue)) {
        if ($homeChild.Name -eq 'node.key') { continue }
        Remove-Item -LiteralPath $homeChild.FullName -Recurse -Force
    }
    Write-Host "Kept machine identity key at $NodeKeyPath."
} else {
    Remove-Item -Recurse -Force -LiteralPath $InstallRoot
}
$pointerFull = if ($pointer) { [IO.Path]::GetFullPath($pointer).TrimEnd('\') } else { $null }
if ($pointerFull -and [string]::Equals($pointerFull, $InstallRoot, [StringComparison]::OrdinalIgnoreCase)) {
    Remove-Item -Path $PointerKey -Force
    $pointerParent = Split-Path -Parent $PointerKey
    $parent = if ($pointerParent) { Get-Item -Path $pointerParent -ErrorAction SilentlyContinue } else { $null }
    if ($parent -and $parent.SubKeyCount -eq 0 -and $parent.ValueCount -eq 0) { Remove-Item -Path $pointerParent }
}
$UsbTempRoot = Join-Path ([IO.Path]::GetTempPath()) 'oxideswarm-usb'
if (Test-Path -LiteralPath $UsbTempRoot) {
    $usbTempItem = Get-Item -LiteralPath $UsbTempRoot -Force
    if (($usbTempItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        Write-Warning "Refusing to remove reparse point at $UsbTempRoot"
    } else {
        $hasReparseDescendant = $false
        foreach ($child in Get-ChildItem -LiteralPath $UsbTempRoot -Recurse -Force -ErrorAction SilentlyContinue) {
            if (($child.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                $hasReparseDescendant = $true
                break
            }
        }
        if ($hasReparseDescendant) {
            Write-Warning "Refusing to remove $UsbTempRoot because it contains a reparse point."
        } else {
            Remove-Item -LiteralPath $UsbTempRoot -Recurse -Force
        }
    }
}
Write-Host "Removed. The cluster owner can also remove this machine from the cluster."
