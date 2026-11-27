<#
.SYNOPSIS
    HOST-1 OS overhead benchmark harness for Windows (design 0114, llama.cpp b12810).
.DESCRIPTION
    Collects non-identifying hardware/OS telemetry and runs llama-bench on Windows
    for State 1 (desktop as used) and State 1b (desktop tidied). Never emits host
    names, user names, or IP addresses into stdout or output files.
#>
[CmdletBinding()]
param(
    [ValidateSet('state1', 'state1b', 'custom')]
    [string]$State = 'state1',

    [string]$CpuBenchDir = 'C:\ox_tmp\host1\bin\win-cpu-x64',
    [string]$VulkanBenchDir = 'C:\ox_tmp\host1\bin\win-vulkan-x64',
    [string]$CudaBenchDir = 'C:\ox_tmp\host1\bin\win-cuda-13.5-x64',
    [string]$Model06B = 'C:\ox_tmp\host1\models\Qwen3-0.6B-Q8_0.gguf',
    [string]$Model8B = 'C:\ox_tmp\host1\models\Qwen3-8B-Q4_K_M.gguf',
    [string]$OutDir = '',

    [int]$Reps = 5,
    [int]$PromptTokens = 512,
    [int]$GenTokens = 128,

    [switch]$Tuned,
    [switch]$CpuOnly,
    [string]$CpuMask = '',
    [int]$Prio = 2,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Force Normal priority for the benchmark driver itself (task rule exception).
try {
    [System.Diagnostics.Process]::GetCurrentProcess().PriorityClass = 'Normal'
} catch {
    Write-Warning "Could not set process priority class to Normal: $($_.Exception.Message)"
}

$UtcNow = [DateTimeOffset]::UtcNow
if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $Stamp = $UtcNow.ToString('yyyyMMddTHHmmssZ')
    $OutDir = Join-Path 'C:\ox_tmp\host1-results' "$Stamp-$State"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Get-NvidiaSmiInfo {
    $cmd = Get-Command 'nvidia-smi' -ErrorAction SilentlyContinue
    if (-not $cmd) {
        return $null
    }
    try {
        $raw = & $cmd.Source --query-gpu=name,driver_version,memory.total,memory.free --format=csv,noheader,nounits 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $raw) {
            return $null
        }
        $first = ($raw | Select-Object -First 1).ToString().Trim()
        $parts = $first -split ',' | ForEach-Object { $_.Trim() }
        if ($parts.Count -lt 4) {
            return $null
        }
        return [ordered]@{
            gpu_name = $parts[0]
            gpu_driver_version = $parts[1]
            vram_total_mib = [int]$parts[2]
            vram_free_mib = [int]$parts[3]
        }
    } catch {
        return $null
    }
}

function Get-ActivePowerPlan {
    try {
        $line = (powercfg /getactivescheme 2>$null | Select-Object -First 1)
        if ($line) {
            return $line.ToString().Trim()
        }
    } catch {}
    return 'unknown'
}

function Get-OptionalFileSha256([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return $null
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $stream = [System.IO.File]::OpenRead($Path)
    try {
        $bytes = $sha.ComputeHash($stream)
        return ([System.BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant())
    } finally {
        $stream.Dispose()
        $sha.Dispose()
    }
}

function Write-Utf8NoBom([string]$Path, [string]$Text) {
    $enc = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Text, $enc)
}

function Scrub-BenchJson([string]$JsonPath) {
    if (-not (Test-Path -LiteralPath $JsonPath -PathType Leaf)) {
        return
    }
    $raw = Get-Content -LiteralPath $JsonPath -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return
    }
    $parsed = $raw | ConvertFrom-Json
    if ($parsed -is [System.Collections.IEnumerable] -and -not ($parsed -is [string])) {
        foreach ($row in $parsed) {
            if ($null -ne $row -and $row.PSObject.Properties['model_filename']) {
                $row.model_filename = [System.IO.Path]::GetFileName([string]$row.model_filename)
            }
        }
    } elseif ($null -ne $parsed -and $parsed.PSObject.Properties['model_filename']) {
        $parsed.model_filename = [System.IO.Path]::GetFileName([string]$parsed.model_filename)
    }
    $outJson = $parsed | ConvertTo-Json -Depth 12
    Write-Utf8NoBom $JsonPath $outJson
}

$Cpus = @(Get-CimInstance Win32_Processor)
$CpuModel = ($Cpus | Select-Object -First 1).Name.Trim()
$PhysicalCores = [int](($Cpus | Measure-Object -Property NumberOfCores -Sum).Sum)
$LogicalProcessors = [int](($Cpus | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum)

$CompSys = Get-CimInstance Win32_ComputerSystem
$RamMib = [math]::Round(($CompSys.TotalPhysicalMemory / 1MB), 1)
$OsInfo = Get-CimInstance Win32_OperatingSystem
$PowerPlan = Get-ActivePowerPlan
$GpuBeforeLoad = Get-NvidiaSmiInfo

$ThreadCandidates = @(1, 4, 8, $PhysicalCores) | Where-Object { $_ -gt 0 -and $_ -le $LogicalProcessors } | Sort-Object -Unique
$ThreadCsv = ($ThreadCandidates -join ',')

$CpuArgs = @('-m', $Model06B, '-ngl', '0', '-t', $ThreadCsv, '-p', "$PromptTokens", '-n', "$GenTokens", '-r', "$Reps", '-o', 'json')
$GpuArgs = @('-m', $Model8B, '-ngl', '99', '-p', "$PromptTokens", '-n', "$GenTokens", '-r', "$Reps", '-o', 'json')
if ($Tuned) {
    if (-not [string]::IsNullOrWhiteSpace($CpuMask)) {
        $CpuArgs += @('-C', $CpuMask)
    }
    $CpuArgs += @('--cpu-strict', '1', '--prio', "$Prio")
    $GpuArgs += @('--prio', "$Prio")
}

function Format-SafeCommand([string]$ExeLeaf, [string[]]$ArgList, [string]$ModelPath) {
    $safe = @($ExeLeaf)
    for ($i = 0; $i -lt $ArgList.Count; $i++) {
        if ($ArgList[$i] -eq $ModelPath) {
            $safe += [System.IO.Path]::GetFileName($ModelPath)
        } else {
            $safe += $ArgList[$i]
        }
    }
    return ($safe -join ' ')
}

$Model06BName = [System.IO.Path]::GetFileName($Model06B)
$Model8BName = $(if ($CpuOnly) { $null } else { [System.IO.Path]::GetFileName($Model8B) })

$Planned = [ordered]@{
    cpu = Format-SafeCommand 'llama-bench.exe' $CpuArgs $Model06B
    gpu_cuda = $(if ($CpuOnly) { $null } else { Format-SafeCommand 'llama-bench.exe' $GpuArgs $Model8B })
    gpu_vulkan = $(if ($CpuOnly) { $null } else { Format-SafeCommand 'llama-bench.exe' $GpuArgs $Model8B })
}

$Facts = [ordered]@{
    utc_timestamp = $UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    state = $State
    cpu_only = [bool]$CpuOnly
    tuned = [bool]$Tuned
    dry_run = [bool]$DryRun
    engine_tag = 'b12810'
    os_caption = $OsInfo.Caption.Trim()
    os_version = $OsInfo.Version.Trim()
    os_build = $OsInfo.BuildNumber.Trim()
    cpu_model = $CpuModel
    physical_cores = $PhysicalCores
    logical_processors = $LogicalProcessors
    ram_mib = $RamMib
    gpu_name = $(if ($GpuBeforeLoad) { $GpuBeforeLoad.gpu_name } else { $null })
    gpu_driver_version = $(if ($GpuBeforeLoad) { $GpuBeforeLoad.gpu_driver_version } else { $null })
    vram_total_mib = $(if ($GpuBeforeLoad) { $GpuBeforeLoad.vram_total_mib } else { $null })
    vram_free_before_load_mib = $(if ($GpuBeforeLoad) { $GpuBeforeLoad.vram_free_mib } else { $null })
    vram_free_before_cuda_mib = $null
    vram_free_before_vulkan_mib = $null
    power_plan = $PowerPlan
    process_priority = [string]([System.Diagnostics.Process]::GetCurrentProcess().PriorityClass)
    thread_sweep = $ThreadCsv
    models = [ordered]@{
        qwen3_0_6b = [ordered]@{
            filename = $Model06BName
            sha256 = Get-OptionalFileSha256 $Model06B
        }
        qwen3_8b = $(if ($CpuOnly) {
            $null
        } else {
            [ordered]@{
                filename = $Model8BName
                sha256 = Get-OptionalFileSha256 $Model8B
            }
        })
    }
    planned_commands = $Planned
}

$FactsPath = Join-Path $OutDir 'machine_facts.json'

if ($DryRun) {
    Write-Utf8NoBom $FactsPath ($Facts | ConvertTo-Json -Depth 8)
    Write-Output "HOST-1 dry run complete."
    Write-Output "STATE=$State CPU=$CpuModel CORES=$PhysicalCores/$LogicalProcessors"
    Write-Output "OUT_DIR=$OutDir"
    exit 0
}

function Resolve-BenchExe([string]$Dir) {
    $direct = Join-Path $Dir 'llama-bench.exe'
    if (Test-Path -LiteralPath $direct -PathType Leaf) {
        return $direct
    }
    $nested = Get-ChildItem -Path $Dir -Filter 'llama-bench.exe' -Recurse -File -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($nested) {
        return $nested.FullName
    }
    throw "llama-bench.exe not found under '$Dir'"
}

if (-not (Test-Path -LiteralPath $Model06B -PathType Leaf)) {
    throw "Model file not found: $([System.IO.Path]::GetFileName($Model06B))"
}
if (-not $CpuOnly -and -not (Test-Path -LiteralPath $Model8B -PathType Leaf)) {
    throw "Model file not found: $([System.IO.Path]::GetFileName($Model8B))"
}

$CpuExe = Resolve-BenchExe $CpuBenchDir

function Invoke-BenchRun([string]$ExePath, [string]$ExtraPathDir, [string[]]$BenchArgs, [string]$JsonOutPath) {
    $PrevPath = $env:PATH
    try {
        if (-not [string]::IsNullOrWhiteSpace($ExtraPathDir)) {
            $env:PATH = "$ExtraPathDir;$PrevPath"
        }
        $ErrLog = "$JsonOutPath.stderr.log"
        $TmpOut = "$JsonOutPath.tmp"
        & $ExePath @BenchArgs 1> $TmpOut 2> $ErrLog
        $rc = $LASTEXITCODE
        if ($rc -ne 0) {
            throw "llama-bench failed with exit code $rc (see $([System.IO.Path]::GetFileName($ErrLog)))"
        }
        Move-Item -Force -LiteralPath $TmpOut -Destination $JsonOutPath
        Scrub-BenchJson $JsonOutPath
    } finally {
        $env:PATH = $PrevPath
    }
}

Write-Output "[1/3] Running CPU benchmark ($Model06BName, threads=$ThreadCsv)..."
Invoke-BenchRun $CpuExe (Split-Path -Parent $CpuExe) $CpuArgs (Join-Path $OutDir 'cpu_qwen3_0.6b_q8_0.json')

if (-not $CpuOnly) {
    $CudaExe = Resolve-BenchExe $CudaBenchDir
    $VulkanExe = Resolve-BenchExe $VulkanBenchDir

    $VramCuda = Get-NvidiaSmiInfo
    if ($VramCuda) { $Facts.vram_free_before_cuda_mib = $VramCuda.vram_free_mib }
    Write-Output "[2/3] Running CUDA benchmark ($Model8BName, ngl=99)..."
    Invoke-BenchRun $CudaExe (Split-Path -Parent $CudaExe) $GpuArgs (Join-Path $OutDir 'gpu_cuda_qwen3_8b_q4_k_m.json')

    $VramVulkan = Get-NvidiaSmiInfo
    if ($VramVulkan) { $Facts.vram_free_before_vulkan_mib = $VramVulkan.vram_free_mib }
    Write-Output "[3/3] Running Vulkan benchmark ($Model8BName, ngl=99)..."
    Invoke-BenchRun $VulkanExe (Split-Path -Parent $VulkanExe) $GpuArgs (Join-Path $OutDir 'gpu_vulkan_qwen3_8b_q4_k_m.json')
} else {
    Write-Output "[CpuOnly] Skipped CUDA and Vulkan GPU benchmarks."
}

Write-Utf8NoBom $FactsPath ($Facts | ConvertTo-Json -Depth 8)
Write-Output "HOST-1 Windows run finished ($State)."
Write-Output "OUT_DIR=$OutDir"
