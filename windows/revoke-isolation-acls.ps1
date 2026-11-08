<#
.SYNOPSIS
    Revokes all Windows NTFS DACL entries granted to the `oxideswarm.toolchain` capability SID
    by `rusty_grid_worker --isolation fs|fs+net`, and deletes any leftover `oxideswarm.task.*`
    AppContainer profiles.

.DESCRIPTION
    When `rusty_grid_worker` starts on Windows with `--isolation fs` or `--isolation fs+net`, it
    records every toolchain directory whose DACL it modifies in
    `~/.oxideswarm/windows_acl_grants.txt` (or `$env:RUSTY_GRID_ACL_GRANT_LOG`) before adding read
    and execute ACEs for the custom capability SID `oxideswarm.toolchain`
    (derived via `DeriveCapabilitySidsFromName`).

    Running this script removes the `oxideswarm.toolchain` SID from every path listed in the
    manifest (and any extra `-Paths` passed on the command line) so that turning off isolation or
    uninstalling OxideSwarm leaves zero permanent ACL changes behind.

.PARAMETER ManifestPath
    Path to the ACL grant manifest written by the worker. Defaults to
    `$env:RUSTY_GRID_ACL_GRANT_LOG` if set, otherwise `$env:USERPROFILE\.oxideswarm\windows_acl_grants.txt`.

.PARAMETER Paths
    Optional explicit paths from which to remove the `oxideswarm.toolchain` capability SID.

.PARAMETER RemoveManifest
    Delete the manifest file after successfully revoking all recorded ACEs.

.PARAMETER DryRun
    Print the `icacls` commands that would be executed without modifying any ACLs.
#>
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string[]]$Paths = @(),
    [switch]$RemoveManifest,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class OxideSwarmIsolationNative {
    [DllImport("userenv.dll", CharSet = CharSet.Unicode, SetLastError = true)]
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

$toolchainSid = [OxideSwarmIsolationNative]::GetToolchainCapabilitySid()
Write-Host "oxideswarm.toolchain capability SID: $toolchainSid"

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    if (-not [string]::IsNullOrWhiteSpace($env:RUSTY_GRID_ACL_GRANT_LOG)) {
        $ManifestPath = $env:RUSTY_GRID_ACL_GRANT_LOG
    } else {
        $ManifestPath = Join-Path $env:USERPROFILE ".oxideswarm\windows_acl_grants.txt"
    }
}

$targetPaths = New-Object System.Collections.Generic.List[string]
foreach ($p in $Paths) {
    if (-not [string]::IsNullOrWhiteSpace($p) -and -not $targetPaths.Contains($p)) {
        $targetPaths.Add($p)
    }
}

if (Test-Path -LiteralPath $ManifestPath) {
    foreach ($line in Get-Content -LiteralPath $ManifestPath) {
        $trimmed = $line.Trim()
        if (-not [string]::IsNullOrWhiteSpace($trimmed) -and -not $targetPaths.Contains($trimmed)) {
            $targetPaths.Add($trimmed)
        }
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
        Write-Host "[DryRun] icacls `"$target`" /remove `"*$toolchainSid`" /t /c /q"
    } else {
        Write-Host "Revoking *$toolchainSid on: $target"
        if ($item.PSIsContainer) {
            & icacls.exe $target /remove "*$toolchainSid" /t /c /q | Out-Null
        } else {
            & icacls.exe $target /remove "*$toolchainSid" /c /q | Out-Null
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

if ($RemoveManifest -and -not $DryRun -and (Test-Path -LiteralPath $ManifestPath)) {
    Remove-Item -LiteralPath $ManifestPath -Force
    Write-Host "Removed manifest: $ManifestPath"
}
