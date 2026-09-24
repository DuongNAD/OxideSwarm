@echo off
setlocal EnableDelayedExpansion
title OxideSwarm Windows Agent Node Runner

if "%~1"=="--help" goto HELP
if "%~1"=="-h" goto HELP
if "%~1"=="/?" goto HELP

echo ========================================================
echo   OxideSwarm Windows Agent Node Runner (CMD / Batch)
echo ========================================================
echo.

set "HUB_URL=%~1"
if "%HUB_URL%"=="" set "HUB_URL=ws://127.0.0.1:8088/ws"

set "NODE_ID=%~2"
if "%NODE_ID%"=="" set "NODE_ID=node-win-%COMPUTERNAME%"

echo Target Hub: %HUB_URL%
echo Node ID:    %NODE_ID%
echo Platform:   windows
echo.

:LOOP
if exist "target\release\agent-mesh.exe" (
    echo [INFO] Running native release binary [agent-mesh]...
    target\release\agent-mesh.exe node --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
) else if exist "target\debug\agent-mesh.exe" (
    echo [INFO] Running native debug binary [agent-mesh]...
    target\debug\agent-mesh.exe node --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
) else if exist "%~dp0..\..\target\release\agent-mesh.exe" (
    echo [INFO] Running native release binary [agent-mesh]...
    "%~dp0..\..\target\release\agent-mesh.exe" node --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
) else if exist "%~dp0..\..\target\debug\agent-mesh.exe" (
    echo [INFO] Running native debug binary [agent-mesh]...
    "%~dp0..\..\target\debug\agent-mesh.exe" node --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
) else (
    echo [INFO] Native binary not found. Running Python fallback client...
    if exist "scripts\agent_node.py" (
        python scripts\agent_node.py --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
    ) else if exist "%~dp0..\..\scripts\agent_node.py" (
        python "%~dp0..\..\scripts\agent_node.py" --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
    ) else (
        python ..\..\scripts\agent_node.py --hub "%HUB_URL%" --id "%NODE_ID%" --platform windows
    )
)

echo [WARN] Agent disconnected or exited. Reconnecting in 3 seconds...
timeout /t 3 >nul
goto LOOP

:HELP
echo ========================================================
echo   OxideSwarm Windows Agent Node Runner (CMD / Batch)
echo ========================================================
echo.
echo Usage: %~nx0 [HUB_URL] [NODE_ID]
echo.
echo Options:
echo   --help, -h, /?    Show this help message and exit.
echo.
echo Arguments:
echo   HUB_URL           WebSocket URL of the OxideRelay Hub (default: ws://127.0.0.1:8088/ws)
echo   NODE_ID           Unique identifier for this node (default: node-win-%%COMPUTERNAME%%)
echo.
exit /b 0
