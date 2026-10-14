@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  OxideSwarm Windows Background Worker Launcher
rem
rem    start_worker.cmd [MASTER_ADDR] [extra rusty-grid arguments, e.g. --no-gpu]
rem
rem  MASTER_ADDR is the master's CLUSTER port, HOST:8088 (never the dashboard's
rem  8080), or "auto" (default) to discover a master on the LAN.
rem  The worker needs the master's cluster secret file at
rem  %USERPROFILE%\.oxideswarm\cluster.secret (copy it with scp or a USB stick);
rem  without it the worker refuses to follow a remote or auto-discovered master.
rem
rem  The worker looks for a GPU itself, but can only use one if rusty-grid.exe was
rem  built with the wgpu engine (cargo build --release --bin rusty-grid --features
rem  gpu-wgpu, or build_windows_worker.sh --gpu). Without it no GPU is advertised,
rem  also with --gpu; the "GPU:" line in worker.log says which case you are in.
rem  Pass --no-gpu to keep the graphics card out of the grid.
rem  Process id and logs live in %USERPROFILE%\.oxideswarm\run (see oxctl.ps1).
rem ==============================================================================

set "SCRIPT_DIR=%~dp0"
set "MASTER_ADDR=auto"
set "OX_EXTRA_ARGS="

rem The first argument is the master address unless it is an option (starts with "-").
set "FIRST=%~1"
if not defined FIRST goto collect
if "%FIRST:~0,1%"=="-" goto collect
set "MASTER_ADDR=%FIRST%"
shift

rem Everything after MASTER_ADDR goes to rusty-grid verbatim.

:collect
if "%~1"=="" goto run
set "OX_EXTRA_ARGS=%OX_EXTRA_ARGS% %~1"
shift
goto collect

:run
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%oxctl.ps1" start worker -MasterAddr "%MASTER_ADDR%"
exit /b %errorlevel%
