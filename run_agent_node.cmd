@echo off
setlocal EnableDelayedExpansion
set "SCRIPT_DIR=%~dp0"
if exist "%SCRIPT_DIR%packaging\agent_mesh\windows\run_agent_node.cmd" (
    call "%SCRIPT_DIR%packaging\agent_mesh\windows\run_agent_node.cmd" %*
) else if exist "%SCRIPT_DIR%windows\run_agent_node.cmd" (
    call "%SCRIPT_DIR%windows\run_agent_node.cmd" %*
) else (
    echo [ERROR] Could not locate packaging\agent_mesh\windows\run_agent_node.cmd
    exit /b 1
)
