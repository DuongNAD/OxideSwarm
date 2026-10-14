@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  OxideSwarm Dynamic Role Switcher for Windows (Master <-> Worker on Demand)
rem
rem    switch_role.cmd master [--lan-dashboard] [--p2p] [--no-worker]
rem    switch_role.cmd worker [MASTER_IP:8088 | auto] [extra rusty-grid arguments]
rem    switch_role.cmd status
rem    switch_role.cmd stop
rem
rem  Only processes started by these launchers (PID files in
rem  %USERPROFILE%\.oxideswarm\run) are ever stopped. The cluster secret
rem  (%USERPROFILE%\.oxideswarm\cluster.secret) must be shared: the master creates it
rem  on first start, workers need a copy (scp or USB). See ..\docs\SECURITY.md.
rem ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "TARGET_ARG=%~1"
if not "%TARGET_ARG%"=="" goto handle_args

:show_menu
cls
echo ==============================================================================
echo             OxideSwarm Role Switcher (Windows Node Coordinator)
echo ==============================================================================
echo  Host          : %COMPUTERNAME%
echo ==============================================================================
echo.
echo  [1] Chuyen may nay thanh MASTER (Dieu phoi cum; dashboard chi tren may nay)
echo  [2] Chuyen may nay thanh WORKER (May tram tinh toan phu tro)
echo  [3] Kiem tra trang thai chi tiet va Log
echo  [4] Tat cac tien trinh do script nay chay (Stop)
echo  [5] Thoat
echo.
set /p "CHOICE=Nhap lua chon [1-5]: "

if "%CHOICE%"=="1" goto menu_master
if "%CHOICE%"=="2" goto menu_worker
if "%CHOICE%"=="3" goto do_status
if "%CHOICE%"=="4" goto do_stop
if "%CHOICE%"=="5" exit /b 0
goto show_menu

:menu_master
set "MASTER_FLAGS="
set "ANS="
set /p "ANS=Mo dashboard cho dien thoai/LAN (--lan-dashboard)? [y/N]: "
if /i "%ANS%"=="y" set "MASTER_FLAGS=%MASTER_FLAGS% --lan-dashboard"
set "ANS="
set /p "ANS=Bat P2P cho may o mang khac (--p2p)? [y/N]: "
if /i "%ANS%"=="y" set "MASTER_FLAGS=%MASTER_FLAGS% --p2p"
goto do_master

:menu_worker
set "CUSTOM_MASTER="
set /p "CUSTOM_MASTER=Nhap dia chi Master dang IP:8088 [Enter de tu dong quet 'auto']: "
if not defined CUSTOM_MASTER set "CUSTOM_MASTER=auto"
goto do_worker

:handle_args
if /i "%TARGET_ARG%"=="master" goto args_master
if /i "%TARGET_ARG%"=="worker" goto args_worker
if /i "%TARGET_ARG%"=="status" goto do_status
if /i "%TARGET_ARG%"=="stop" goto do_stop
echo [ERROR] Unknown command: %TARGET_ARG%
echo Usage: switch_role.cmd [master [--lan-dashboard] [--p2p] [--no-worker] ^| worker [MASTER_IP:8088] ^| status ^| stop]
exit /b 1

:args_master
shift
set "MASTER_FLAGS="
:args_master_loop
if "%~1"=="" goto do_master
set "MASTER_FLAGS=%MASTER_FLAGS% %~1"
shift
goto args_master_loop

:args_worker
shift
set "CUSTOM_MASTER=%~1"
if not defined CUSTOM_MASTER set "CUSTOM_MASTER=auto"
set "WORKER_REST="
if not "%~1"=="" shift
:args_worker_loop
if "%~1"=="" goto do_worker
set "WORKER_REST=%WORKER_REST% %~1"
shift
goto args_worker_loop

rem ------------------------------------------------------------------------------
rem  Role: MASTER
rem ------------------------------------------------------------------------------
:do_master
echo.
echo [INFO] Dang chuyen doi sang vai tro MASTER...
call "%SCRIPT_DIR%start_master.cmd"%MASTER_FLAGS%
if errorlevel 1 goto failed
echo.
echo   * KET NOI CAC MAY KHAC (cung LAN) sau khi da chep cluster.secret sang may do:
echo       Windows : switch_role.cmd worker IP-MAY-NAY:8088
echo       Mac/Linux: ./switch_role.sh worker IP-MAY-NAY:8088
echo   * May o mang khac: chay lai voi --p2p va dung ticket cung connect_remote.
if "%TARGET_ARG%"=="" pause
exit /b 0

rem ------------------------------------------------------------------------------
rem  Role: WORKER
rem ------------------------------------------------------------------------------
:do_worker
echo.
echo [INFO] Dang chuyen doi sang vai tro WORKER...
call "%SCRIPT_DIR%start_worker.cmd" "%CUSTOM_MASTER%"%WORKER_REST%
if errorlevel 1 goto failed
if "%TARGET_ARG%"=="" pause
exit /b 0

rem ------------------------------------------------------------------------------
rem  Role: STATUS / STOP
rem ------------------------------------------------------------------------------
:do_status
call "%SCRIPT_DIR%status_worker.cmd"
exit /b 0

:do_stop
echo [INFO] Dang tat cac tien trinh OxideSwarm do script nay chay...
call "%SCRIPT_DIR%stop_worker.cmd" all
if "%TARGET_ARG%"=="" pause
exit /b 0

:failed
echo.
echo [ERROR] Khong khoi dong duoc. Xem log trong %USERPROFILE%\.oxideswarm\run
if "%TARGET_ARG%"=="" pause
exit /b 1
