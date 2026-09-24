<#
.SYNOPSIS
    Convenience wrapper for Windows Agent Node Runner
#>
param(
    [string]$Hub = "ws://127.0.0.1:8088/ws",
    [string]$NodeId = ("node-windows-" + $env:COMPUTERNAME.ToLower()),
    [string]$BinaryPath = ""
)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$ScriptDir\windows\run_agent_node.ps1" -Hub $Hub -NodeId $NodeId -BinaryPath $BinaryPath
