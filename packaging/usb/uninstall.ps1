# OxideSwarm USB uninstaller for Windows (kit format v0, design 0053 / 0055).
# Stops the worker this kit installed, revokes any oxideswarm.toolchain ACL grants recorded in its
# manifest (<install folder>\home\windows_acl_grants.txt), removes its HKCU Run entry, its folder
# (binaries, the cluster secret it received, logs, task folders and file cache), and
# %TEMP%\oxideswarm-usb. Nothing else is touched. Without -InstallRoot it removes the install the
# installer recorded in HKCU (another drive included), else %LOCALAPPDATA%\OxideSwarm.
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1 [-InstallRoot DIR] [-Yes]
[CmdletBinding()]
param(
    [string]$InstallRoot = "",
    [switch]$Yes
)
$ErrorActionPreference = 'Stop'
$PointerKey = 'HKCU:\Software\OxideSwarm\UsbKit'
$pointer = $null
try { $pointer = (Get-ItemProperty -Path $PointerKey -Name InstallRoot -ErrorAction Stop).InstallRoot } catch { $pointer = $null }
if (-not $InstallRoot) { $InstallRoot = if ($pointer) { $pointer } else { Join-Path $env:LOCALAPPDATA "OxideSwarm" } }
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
$Exe = Join-Path $InstallRoot 'bin\oxideswarm.exe'
if (-not (Test-Path -LiteralPath $InstallRoot)) { Write-Host "Nothing to remove: $InstallRoot does not exist."; exit 0 }
if (-not (Test-Path -LiteralPath $Exe)) { Write-Host "ERROR: $InstallRoot does not look like a USB-kit install; not touching it." -ForegroundColor Red; exit 1 }
# Only a folder this kit made is deleted: it carries the installer's marker (installs from before the
# marker existed only in the default folder), it is not a link, and nothing inside is one, because
# Windows PowerShell 5.1 follows junctions while it deletes.
$rootItem = Get-Item -LiteralPath $InstallRoot -Force
if ($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { Write-Host "ERROR: $InstallRoot is a link; not touching it." -ForegroundColor Red; exit 1 }
$isDefault = $InstallRoot -eq (Join-Path $env:LOCALAPPDATA 'OxideSwarm').TrimEnd('\')
if (-not $isDefault -and -not (Test-Path -LiteralPath (Join-Path $InstallRoot '.oxideswarm-usb-install'))) {
    Write-Host "ERROR: $InstallRoot has no .oxideswarm-usb-install marker; not touching it." -ForegroundColor Red; exit 1
}
$links = @(Get-ChildItem -LiteralPath $InstallRoot -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint } | Select-Object -First 3)
if ($links.Count -gt 0) {
    Write-Host "ERROR: $InstallRoot contains links ($(($links | ForEach-Object { $_.FullName }) -join ', ')); remove them first, then run the uninstaller again." -ForegroundColor Red; exit 1
}
if (-not $Yes) {
    $ans = Read-Host "Remove the OxideSwarm worker installed in $InstallRoot? [y/N]"
    if ($ans -notmatch '^(y|yes)$') { Write-Host "Nothing removed."; exit 0 }
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

Remove-Item -Recurse -Force -LiteralPath $InstallRoot
if ($pointer -and $pointer -eq $InstallRoot) {
    Remove-Item -Path $PointerKey -Force
    $parent = Get-Item -Path 'HKCU:\Software\OxideSwarm' -ErrorAction SilentlyContinue
    if ($parent -and $parent.SubKeyCount -eq 0 -and $parent.ValueCount -eq 0) { Remove-Item -Path 'HKCU:\Software\OxideSwarm' }
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
