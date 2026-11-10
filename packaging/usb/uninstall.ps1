# OxideSwarm USB uninstaller for Windows (kit format v0, design 0053 / 0055).
# Stops the worker this kit installed, revokes any oxideswarm.toolchain ACL grants recorded in its
# manifest (<install folder>\home\windows_acl_grants.txt), removes its HKCU Run entry and its
# folder (binaries, the cluster secret it received, logs). Nothing else is touched.
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1 [-InstallRoot DIR] [-Yes]
[CmdletBinding()]
param(
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA "OxideSwarm"),
    [switch]$Yes
)
$ErrorActionPreference = 'Stop'
$Exe = Join-Path $InstallRoot 'bin\oxideswarm.exe'
if (-not (Test-Path $InstallRoot)) { Write-Host "Nothing to remove: $InstallRoot does not exist."; exit 0 }
if (-not (Test-Path $Exe)) { Write-Host "ERROR: $InstallRoot does not look like a USB-kit install; not touching it." -ForegroundColor Red; exit 1 }
if (-not $Yes) {
    $ans = Read-Host "Remove the OxideSwarm worker installed in $InstallRoot? [y/N]"
    if ($ans -notmatch '^(y|yes)$') { Write-Host "Nothing removed."; exit 0 }
}
$runValueName = if ($env:OXIDESWARM_USB_RUN_VALUE) { $env:OXIDESWARM_USB_RUN_VALUE } else { 'OxideSwarmUsbWorker' }
Remove-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' -Name $runValueName -ErrorAction SilentlyContinue
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
    & $RevokeScript -ManifestPath $Manifest -RemoveManifest
    if ($LASTEXITCODE -ne 0 -or (Test-Path -LiteralPath $Manifest)) {
        Write-Host "ERROR: Isolation ACL revocation did not complete (exit code $LASTEXITCODE); keeping $InstallRoot and $Manifest." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "No isolation ACL grant manifest in $Manifest (nothing to revoke)."
}

Remove-Item -Recurse -Force $InstallRoot
Write-Host "Removed. The cluster owner can also remove this machine from the cluster."
