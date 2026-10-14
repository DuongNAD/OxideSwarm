@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  OxideSwarm Windows Background MASTER Launcher
rem
rem    start_master.cmd [--lan-dashboard] [--p2p] [--no-worker]
rem
rem  Cluster port : 0.0.0.0:8088 (workers connect here; authenticated by the
rem                 cluster secret, created on first start in
rem                 %USERPROFILE%\.oxideswarm\cluster.secret - copy it to every worker)
rem  Dashboard    : http://127.0.0.1:8080, this PC only. --lan-dashboard opens it to
rem                 phones / other PCs; the login token is printed by
rem                 "rusty-grid secret show --dashboard-token".
rem  --p2p        : also publish a P2P ticket for machines on other networks.
rem  Process id and logs live in %USERPROFILE%\.oxideswarm\run (see oxctl.ps1).
rem ==============================================================================

set "SCRIPT_DIR=%~dp0"
set "OX_FLAGS="

:parse
if "%~1"=="" goto run
if /i "%~1"=="--lan-dashboard" goto flag_lan
if /i "%~1"=="--p2p" goto flag_p2p
if /i "%~1"=="--no-worker" goto flag_noworker
echo [ERROR] Unknown option: %~1
echo Usage: start_master.cmd [--lan-dashboard] [--p2p] [--no-worker]
exit /b 1

:flag_lan
set "OX_FLAGS=%OX_FLAGS% -DashboardLan"
shift
goto parse

:flag_p2p
set "OX_FLAGS=%OX_FLAGS% -P2p"
shift
goto parse

:flag_noworker
set "OX_FLAGS=%OX_FLAGS% -NoWorker"
shift
goto parse

:run
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%oxctl.ps1" start master%OX_FLAGS%
exit /b %errorlevel%
