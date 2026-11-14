<#
.SYNOPSIS
    Revokes all Windows NTFS DACL entries granted to the `oxideswarm.toolchain` capability SID
    by `rusty_grid_worker --isolation fs|fs+net`, restores the original DACL on `sandbox_base`,
    and deletes leftover `oxideswarm.task.*` AppContainer profiles when no worker is running.

.DESCRIPTION
    When `rusty_grid_worker` starts on Windows with `--isolation fs` or `--isolation fs+net`, it
    records every toolchain path and `sandbox_base` original SDDL in
    `<user_home>\.oxideswarm\windows_acl_grants.txt` (or `$env:RUSTY_GRID_ACL_GRANT_LOG`) before
    modifying any DACL for the custom capability SID `oxideswarm.toolchain` (derived via
    `DeriveCapabilitySidsFromName` in `kernelbase.dll`).

    Stop the worker before running this script. Running this script removes the
    `oxideswarm.toolchain` SID (both allow and deny ACEs) from every path listed in the manifest,
    restores the original DACL SDDL on `sandbox_base`, and deletes leftover `oxideswarm.task.*`
    AppContainer profiles so that turning off isolation or uninstalling OxideSwarm leaves zero
    permanent ACL changes behind.

    Supports both Windows PowerShell 5.1 (`powershell.exe`, invoked by the `.cmd` launchers) and
    PowerShell 7+ (`pwsh`).

.PARAMETER ManifestPath
    Path to the ACL grant manifest written by the worker. Defaults to
    `$env:RUSTY_GRID_ACL_GRANT_LOG` if set, otherwise
    `$env:OXIDESWARM_HOME\windows_acl_grants.txt` if `$env:OXIDESWARM_HOME` is set, otherwise
    `$env:USERPROFILE\.oxideswarm\windows_acl_grants.txt`.

.PARAMETER Paths
    Optional explicit paths from which to remove the `oxideswarm.toolchain` capability SID.

.PARAMETER RemoveManifest
    Delete the manifest file after successfully revoking all recorded ACEs.

.PARAMETER DryRun
    Print the actions that would be executed without modifying any ACLs or profiles.
#>
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string[]]$Paths = @(),
    [switch]$RemoveManifest,
    [switch]$DryRun,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSVersion.Major -lt 6 -and $env:SystemRoot) {
    $winPsModules = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\Modules'
    if (Test-Path -LiteralPath $winPsModules) {
        $env:PSModulePath = $winPsModules
    }
}

if (-not ('OxideSwarmIsolationNative' -as [type])) {
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class OxideSwarmIsolationNative {
    [DllImport("kernelbase.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool DeriveCapabilitySidsFromName(
        string CapName,
        out IntPtr CapabilityGroupSids,
        out uint CapabilityGroupSidCount,
        out IntPtr CapabilitySids,
        out uint CapabilitySidCount);

    [DllImport("userenv.dll", CharSet = CharSet.Unicode)]
    public static extern int DeleteAppContainerProfile(string pszAppContainerName);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool ConvertSidToStringSidW(IntPtr Sid, out IntPtr StringSid);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr hMem);

    public static string GetToolchainCapabilitySid() {
        IntPtr groupSids = IntPtr.Zero;
        uint groupCount = 0;
        IntPtr capSids = IntPtr.Zero;
        uint capCount = 0;
        if (!DeriveCapabilitySidsFromName(
            "oxideswarm.toolchain",
            out groupSids,
            out groupCount,
            out capSids,
            out capCount))
        {
            throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
        }
        try {
            if (capCount == 0 || capSids == IntPtr.Zero) {
                throw new InvalidOperationException("DeriveCapabilitySidsFromName returned 0 capability SIDs.");
            }
            IntPtr firstSid = Marshal.ReadIntPtr(capSids);
            IntPtr strPtr = IntPtr.Zero;
            if (!ConvertSidToStringSidW(firstSid, out strPtr) || strPtr == IntPtr.Zero) {
                throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
            }
            try {
                return Marshal.PtrToStringUni(strPtr);
            } finally {
                LocalFree(strPtr);
            }
        } finally {
            if (groupSids != IntPtr.Zero) {
                for (int i = 0; i < (int)groupCount; i++) {
                    IntPtr s = Marshal.ReadIntPtr(groupSids, i * IntPtr.Size);
                    if (s != IntPtr.Zero) LocalFree(s);
                }
                LocalFree(groupSids);
            }
            if (capSids != IntPtr.Zero) {
                for (int i = 0; i < (int)capCount; i++) {
                    IntPtr s = Marshal.ReadIntPtr(capSids, i * IntPtr.Size);
                    if (s != IntPtr.Zero) LocalFree(s);
                }
                LocalFree(capSids);
            }
        }
    }
}
"@
}

$toolchainSid = [OxideSwarmIsolationNative]::GetToolchainCapabilitySid()
Write-Host "oxideswarm.toolchain capability SID: $toolchainSid"

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    if (-not [string]::IsNullOrWhiteSpace($env:RUSTY_GRID_ACL_GRANT_LOG)) {
        $ManifestPath = $env:RUSTY_GRID_ACL_GRANT_LOG
    } elseif (-not [string]::IsNullOrWhiteSpace($env:OXIDESWARM_HOME)) {
        $ManifestPath = Join-Path $env:OXIDESWARM_HOME "windows_acl_grants.txt"
    } else {
        $ManifestPath = Join-Path $env:USERPROFILE ".oxideswarm\windows_acl_grants.txt"
    }
}

Write-Host "ManifestPath: $ManifestPath"

$targetPaths = New-Object System.Collections.Generic.List[string]
$sddlRestores = New-Object System.Collections.Generic.List[pscustomobject]

foreach ($p in $Paths) {
    if (-not [string]::IsNullOrWhiteSpace($p) -and -not $targetPaths.Contains($p)) {
        $targetPaths.Add($p)
    }
}

if (Test-Path -LiteralPath $ManifestPath) {
    foreach ($line in Get-Content -LiteralPath $ManifestPath) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        if ($trimmed.StartsWith("SDDL|")) {
            $parts = $trimmed.Split('|', 3)
            if ($parts.Length -eq 3 -and -not [string]::IsNullOrWhiteSpace($parts[1]) -and -not [string]::IsNullOrWhiteSpace($parts[2])) {
                $sddlRestores.Add([pscustomobject]@{
                    Path = $parts[1]
                    Sddl = $parts[2]
                })
            }
        } elseif (-not $targetPaths.Contains($trimmed)) {
            $targetPaths.Add($trimmed)
        }
    }
} else {
    Write-Host "No manifest file found at $ManifestPath"
}

$cimQueryFailed = $false
$runningWorkers = @()
try {
    $runningWorkers = @(Get-CimInstance Win32_Process -ErrorAction Stop |
        Where-Object {
            $_.Name -in @('oxideswarm.exe', 'rusty-grid.exe') -and
            $_.CommandLine -match '(?:oxideswarm|rusty-grid)(?:\.exe)?["'']?\s+(worker)(\s|$)'
        })
} catch {
    $cimQueryFailed = $true
}
$lockHeld = $false
if (-not [string]::IsNullOrWhiteSpace($env:TEMP) -and (Test-Path -LiteralPath $env:TEMP)) {
    foreach ($lf in Get-ChildItem -LiteralPath $env:TEMP -Filter "oxideswarm.task.*.lock" -File -Force -ErrorAction SilentlyContinue) {
        try {
            $fs = [System.IO.File]::Open($lf.FullName, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
            $fs.Close()
        } catch {
            $lockHeld = $true
            break
        }
    }
}

if (($cimQueryFailed -or $runningWorkers.Count -gt 0 -or $lockHeld) -and -not $Force -and -not $DryRun) {
    if ($cimQueryFailed) {
        Write-Warning "Refusing to revoke isolation ACL grants while Win32_Process query failed (worker status unknown); pass -Force to override."
    } else {
        Write-Warning "Refusing to revoke isolation ACL grants, restore sandbox_base SDDL, or sweep oxideswarm.task.* AppContainer profiles while another worker is running; stop all workers first (or pass -Force)."
    }
    exit 2
}

$failures = 0

foreach ($restore in $sddlRestores) {
    $rPath = $restore.Path
    $rSddl = $restore.Sddl
    if (-not (Test-Path -LiteralPath $rPath)) {
        Write-Host "Skipping missing SDDL restore path: $rPath"
        continue
    }
    $item = Get-Item -LiteralPath $rPath -Force
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        Write-Host "Skipping reparse point SDDL restore: $rPath"
        continue
    }
    $leaf = Split-Path -Leaf $rPath
    if ($leaf -notin @('sandboxes', 'workspaces') -or -not $item.PSIsContainer) {
        Write-Warning "Refusing SDDL restore on non-sandbox/workspace base path: $rPath"
        continue
    }
    try {
        $dirInfo = New-Object System.IO.DirectoryInfo($rPath)
        if ('System.IO.FileSystemAclExtensions' -as [type]) {
            $curAcl = [System.IO.FileSystemAclExtensions]::GetAccessControl($dirInfo, [System.Security.AccessControl.AccessControlSections]::Access)
        } else {
            $curAcl = $dirInfo.GetAccessControl([System.Security.AccessControl.AccessControlSections]::Access)
        }
        $curSddl = $curAcl.GetSecurityDescriptorSddlForm([System.Security.AccessControl.AccessControlSections]::Access)
        if ($curSddl -notlike "*$toolchainSid*") {
            Write-Host "Skipping SDDL restore on $rPath (current DACL does not contain *$toolchainSid)"
            continue
        }
        if ($DryRun) {
            Write-Host "[DryRun] Restore-AclSddl `"$rPath`" `"$rSddl`""
        } else {
            Write-Host "Restoring original DACL SDDL on: $rPath"
            $acl = New-Object System.Security.AccessControl.DirectorySecurity
            $acl.SetSecurityDescriptorSddlForm($rSddl, [System.Security.AccessControl.AccessControlSections]::Access)
            if ('System.IO.FileSystemAclExtensions' -as [type]) {
                [System.IO.FileSystemAclExtensions]::SetAccessControl($dirInfo, $acl)
            } else {
                $dirInfo.SetAccessControl($acl)
            }
        }
    } catch {
        Write-Warning "Failed to restore original DACL SDDL on $($rPath): $($_.Exception.Message)"
        $failures++
    }
}

foreach ($target in $targetPaths) {
    if (-not (Test-Path -LiteralPath $target)) {
        Write-Host "Skipping missing path: $target"
        continue
    }
    $item = Get-Item -LiteralPath $target -Force
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        Write-Host "Skipping reparse point: $target"
        continue
    }
    if ($DryRun) {
        Write-Host "[DryRun] icacls `"$target`" /remove `"*$toolchainSid`" /c /q"
    } else {
        Write-Host "Revoking *$toolchainSid on: $target"
        $global:LASTEXITCODE = 0
        & icacls.exe $target /remove "*$toolchainSid" /c /q | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "icacls /remove exited with code $LASTEXITCODE on: $target"
            $failures++
        }
    }
}

$packagesDir = Join-Path $env:LOCALAPPDATA "Packages"
if (Test-Path -LiteralPath $packagesDir) {
    Get-ChildItem -LiteralPath $packagesDir -Directory -Force -ErrorAction SilentlyContinue | ForEach-Object {
        $prefix = ($_.Name -split '_')[0]
        if ($prefix.StartsWith("oxideswarm.task.")) {
            if ($DryRun) {
                Write-Host "[DryRun] DeleteAppContainerProfile($prefix)"
            } else {
                $hr = [OxideSwarmIsolationNative]::DeleteAppContainerProfile($prefix)
                Write-Host "Deleted leftover AppContainer profile $prefix (HRESULT 0x$($hr.ToString('x8')))"
            }
        }
    }
}

if ($failures -gt 0) {
    Write-Warning "Completed with $failures failure(s); keeping manifest: $ManifestPath"
    exit 1
}

if ($RemoveManifest -and -not $DryRun -and (Test-Path -LiteralPath $ManifestPath)) {
    Remove-Item -LiteralPath $ManifestPath -Force
    Write-Host "Removed manifest: $ManifestPath"
}
exit 0
