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
# (%LOCALAPPDATA%\oxideswarm\workspaces). Outside %LOCALAPPDATA%, the install folder holds the task
# folders and file cache too, and the installer limits it to this user, SYSTEM and Administrators.
#
# -Update (what "Update OxideSwarm.cmd" runs) updates an existing install in place: it keeps its
# folder, name, GPU choice, isolation, cluster membership and start at logon, asks nothing, does
# not join again, and only replaces the binaries and restarts the worker.
#
#   powershell -ExecutionPolicy Bypass -File install.ps1 [-Name NAME] [-InstallRoot DIR] [-NoAutostart] [-Workspaces | -NoWorkspaces] [-Isolation off|fs]
#                                                 [-Gpu | -NoGpu] [-Slots N] [-Update]
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
            if (-not $script:PreexistingRoot) {
                Remove-Item -LiteralPath $InstallRoot -Recurse -Force -ErrorAction SilentlyContinue
            } else {
                foreach ($f in @('.oxideswarm-usb-install', 'kit-install.txt', 'start-worker.vbs', 'revoke-isolation-acls.ps1')) {
                    $p = Join-Path $InstallRoot $f
                    if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }
                }
                foreach ($d in @('bin', 'home', 'run', 'logs', 'workspaces')) {
                    $dp = Join-Path $InstallRoot $d
                    if (Test-Path -LiteralPath $dp) { Remove-Item -LiteralPath $dp -Recurse -Force -ErrorAction SilentlyContinue }
                }
            }
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
if ($IsV1Kit -and -not (Test-Path -LiteralPath $KitJson)) { Fail "kit.json is missing from this v1 kit" }
if (Test-Path -LiteralPath $KitJson) {
    try { $kitMeta = Get-Content -Raw -LiteralPath $KitJson | ConvertFrom-Json }
    catch { Fail "invalid kit.json: $_" }
    if ($kitMeta.kit_format -ne 1) { Fail "unsupported kit_format '$($kitMeta.kit_format)' in kit.json (expected 1)" }
    $kitVersion = "$($kitMeta.version)"
    if (-not $kitVersion -or $kitVersion.Contains('..') -or $kitVersion -notmatch '^[A-Za-z0-9.+_-]+$') {
        Fail "invalid version '$kitVersion' in kit.json"
    }
    $kitRoles = @($kitMeta.roles | ForEach-Object { "$_".ToLowerInvariant() })
    $KitHasWorker = $kitRoles -contains 'worker'
    $KitHasClient = $kitRoles -contains 'client'
    if (-not $KitHasWorker -and -not $KitHasClient) { Fail "kit.json roles lists neither 'worker' nor 'client'" }
    if (-not $Update) { $useWorkspaces = [bool]$kitMeta.workspaces }
}
if (-not $KitHasWorker) { $NoGpu = $true }
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
    if ($settings['workspaces'] -eq '0') { $useWorkspaces = $false }
    elseif ($settings['workspaces'] -eq '1') { $useWorkspaces = $true }
    if (-not $IsV1Kit -and $settings.ContainsKey('worker')) { $KitHasWorker = ($settings['worker'] -eq '1') }
    if (-not $IsV1Kit -and $settings.ContainsKey('client')) { $KitHasClient = ($settings['client'] -eq '1') }
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
$explicitRoot = $PSBoundParameters.ContainsKey('InstallRoot')
if (-not $Update -and -not $explicitRoot -and -not [Console]::IsInputRedirected) {
    $choices = @($DefaultRoot)
    foreach ($d in [IO.DriveInfo]::GetDrives()) {
        if ($d.Name -eq [IO.Path]::GetPathRoot($DefaultRoot)) { continue }
        $candidate = Join-Path $d.Name 'OxideSwarm-USB'
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

$expected = Lookup-SumEntry (Get-Content $Sums) $Archive.Name
if (-not $expected) { Fail "$($Archive.Name) is not listed in SHA256SUMS" }
$actual = (Get-FileHash $Archive.FullName -Algorithm SHA256).Hash.ToLower()
if ($expected -ne $actual) { Fail "checksum mismatch for $($Archive.Name): the copy on the stick is damaged" }
Say "  archive: $($Archive.Name) (checksum OK)"
Verify-OnlineSums $Archive.Name $expected

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
} catch {
    Fail "failed while unpacking $($Archive.Name): $_"
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
$workerArgs = "worker --name $Name --no-portal --max-concurrency $slots --isolation $Isolation"
# Release 0.1.1 predates FEAT-3d (--workspaces under Windows --isolation fs), so omit --workspaces on 0.1.0/0.1.1 when -Isolation fs is used.
$supportsIsolatedWorkspaces = ($Isolation -eq 'off') -or ($version -notmatch '\b0\.1\.[01](\b|-)')
if ($useWorkspaces -and $supportsIsolatedWorkspaces -and (Get-Command cargo -ErrorAction SilentlyContinue)) {
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
Say "  connected (isolation: $Isolation)"
}
# What an update keeps: the name, the build, the workspace choice, roles and isolation.
$builtWithGpu = if ($KitHasWorker -and $GpuArchive -and $Archive.FullName -eq $GpuArchive.FullName) { '1' } else { '0' }
$withWorkspaces = if ($useWorkspaces) { '1' } else { '0' }
$withWorkerRole = if ($KitHasWorker) { '1' } else { '0' }
$withClientRole = if ($KitHasClient) { '1' } else { '0' }
$recordedSlots = if ($KitHasWorker -and $slotsSet -ge 1) { "$slotsSet" } else { '' }
Set-Content -Path (Join-Path $InstallRoot 'kit-install.txt') -Encoding ASCII -Value @(
    "name=$Name",
    "gpu=$builtWithGpu",
    "workspaces=$withWorkspaces",
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

# 9. Check the cluster.
if ($KitHasClient) {
    $env:RUST_LOG = 'off'
    Write-Host ""
    Write-Host "Checking the cluster (oxideswarm doctor) ..."
    & $Exe doctor @checkArgs
    $doctor = if ($LASTEXITCODE -eq 0) { "passed" } else { "reported a problem (see above)" }
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
