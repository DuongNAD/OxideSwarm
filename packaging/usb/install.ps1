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
                foreach ($d in @('bin', 'home', 'run', 'logs', 'workspaces', 'work')) {
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
    $hasUnresolvable = $false
    foreach ($h in $explicit) {
        $ok = if ($h.ContainsKey('resolvable')) { [bool]$h.resolvable } else { Test-SidResolvable $h.sid }
        if (-not $ok) { $hasUnresolvable = $true; break }
    }
    if ($hasUnresolvable) {
        $psPath = $problem.dir.Replace("'", "''")
        $purges = ($explicit | ForEach-Object { '$a.PurgeAccessRules([Security.Principal.SecurityIdentifier]''' + $_.sid + ''')' }) -join '; '
        return ('powershell -NoProfile -Command "$item = Get-Item -LiteralPath ''' + $psPath + ''' -Force; $a = $item.GetAccessControl([System.Security.AccessControl.AccessControlSections]::Access); ' + $purges + '; $item.SetAccessControl($a)"')
    }
    $target = $problem.dir
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
if ($KitHasWorker -and $Isolation -eq 'fs' -and $useWorkspaces -and (-not $archivePredatesIsolatedWs) -and ($Workspaces -or [bool](Get-Command cargo -ErrorAction SilentlyContinue)) -and $UserProfileFull) {
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

# Every command below keeps its state in $State and runs from $InstallRoot.
$GrantLog = Join-Path $State 'windows_acl_grants.txt'
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
    if ($null -eq $stoppedAfter -and @($list | Where-Object { $_.name -eq $Name -and $_.tags -contains 'state:connected' }).Count -gt 0) { $connected = $true }
}
if ($pending -and $pending.IsCompleted) { try { $lister.Dispose() } catch { } }
if ($null -ne $stoppedAfter) { Fail-WorkerStart "the worker stopped $stoppedAfter s after it started (exit code $(Get-WorkerExitCode))" }
if (-not $connected) { Fail-WorkerStart "the worker ran for 30 s, but the master did not list it" }
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
