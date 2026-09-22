@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: OxideSwarm Dynamic Role Switcher for Windows (Master <-> Worker on Demand)
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

:: Auto-detect local IP address
for /f "tokens=*" %%a in ('powershell -NoProfile -Command "(Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -like '192.168.*' -or $_.IPAddress -like '10.*' } | Select-Object -First 1).IPAddress"') do (
    set "LOCAL_IP=%%a"
)
if not defined LOCAL_IP set "LOCAL_IP=127.0.0.1"

set "TARGET_ARG=%~1"
if not "%TARGET_ARG%"=="" goto handle_args

:show_menu
cls
echo ==============================================================================
echo             OxideSwarm Role Switcher (Windows Node Coordinator)
echo ==============================================================================
echo  Host IP       : %LOCAL_IP% (DESKTOP-3EPV830)
call :detect_role
echo  Current Role  : [%CURRENT_ROLE%]
echo ==============================================================================
echo.
echo  [1] Chuyen may Case thanh MASTER (Dieu phoi cum + Web Dashboard)
echo  [2] Chuyen may Case thanh WORKER (May tram tinh toan GPU phu tro)
echo  [3] Kiem tra trang thai chi tiet va Log
echo  [4] Tat sach tien trinh (Stop All)
echo  [5] Thoat
echo.
set /p "CHOICE=Nhap lua chon [1-5]: "

if "%CHOICE%"=="1" goto do_master
if "%CHOICE%"=="2" goto do_worker
if "%CHOICE%"=="3" goto do_status
if "%CHOICE%"=="4" goto do_stop
if "%CHOICE%"=="5" exit /b 0
goto show_menu

:handle_args
if /i "%TARGET_ARG%"=="master" goto do_master
if /i "%TARGET_ARG%"=="worker" (
    set "CUSTOM_MASTER=%~2"
    goto do_worker
)
if /i "%TARGET_ARG%"=="status" goto do_status
if /i "%TARGET_ARG%"=="stop" goto do_stop
echo [ERROR] Unknown command: %TARGET_ARG%
echo Usage: switch_role.cmd [master | worker [MASTER_IP:PORT] | status | stop]
exit /b 1

:: ------------------------------------------------------------------------------
:: Role: MASTER
:: ------------------------------------------------------------------------------
:do_master
echo.
echo [INFO] Dang chuyen doi sang vai tro MASTER...
call :kill_existing

echo [OK] Dang khoi chay OxideSwarm Master tren may Case...
call "%SCRIPT_DIR%start_master.cmd"

echo.
echo ==============================================================================
echo [SUCCESS] May Case da tro thanh MASTER thanh cong!
echo ==============================================================================
echo   - Cluster Port : %LOCAL_IP%:8088
echo   - Web UI       : http://%LOCAL_IP%:8080 (hoac http://localhost:8080)
echo.
echo   * HUONG DAN KET NOI CAC MAY KHAC VE MAY CASE NAY:
echo     + Tren Mac     : ./switch_role.sh worker auto (hoac ./switch_role.sh worker %LOCAL_IP%:8088)
echo     + Tren Phone   : Mo trinh duyet vao http://%LOCAL_IP%:8080 (hoac nhan 'Quet Master')
echo ==============================================================================
echo.
if "%TARGET_ARG%"=="" pause
exit /b 0

:: ------------------------------------------------------------------------------
:: Role: WORKER
:: ------------------------------------------------------------------------------
:do_worker
echo.
echo [INFO] Dang chuyen doi sang vai tro WORKER...
call :kill_existing

if not defined CUSTOM_MASTER (
    set "DEFAULT_MASTER=auto"
    if "%TARGET_ARG%"=="" (
        set /p "TARGET_MASTER=Nhap dia chi Master (Enter de tu dong quet UDP 'auto' hoac 192.168.1.144:8088): "
        if not defined TARGET_MASTER set "TARGET_MASTER=!DEFAULT_MASTER!"
    ) else (
        set "TARGET_MASTER=!DEFAULT_MASTER!"
    )
) else (
    set "TARGET_MASTER=%CUSTOM_MASTER%"
)

echo [OK] Dang ket noi may Case (GPU) ve Master tai !TARGET_MASTER!...
call "%SCRIPT_DIR%start_worker.cmd" "!TARGET_MASTER!"

echo.
echo ==============================================================================
echo [SUCCESS] May Case da tro thanh WORKER (windows-case-gpu)!
echo ==============================================================================
echo   - Target Master : !TARGET_MASTER! (ho tro tu dong tim kiem UDP beacon)
echo   - Standby Portal: http://%LOCAL_IP%:8080 (HTTP 307 Redirect ve Master Web UI)
echo   - Hardware      : Auto-detected GPU + CPU + RAM
echo   - Che do        : Chay ngam 100%% (Zero-Window)
echo ==============================================================================
echo.
if "%TARGET_ARG%"=="" pause
exit /b 0

:: ------------------------------------------------------------------------------
:: Role: STATUS
:: ------------------------------------------------------------------------------
:do_status
call "%SCRIPT_DIR%status_worker.cmd"
exit /b 0

:: ------------------------------------------------------------------------------
:: Role: STOP
:: ------------------------------------------------------------------------------
:do_stop
echo [INFO] Dang tat tat ca tien trinh OxideSwarm...
call :kill_existing
echo [OK] Tat ca tien trinh da duoc dung sach se.
if "%TARGET_ARG%"=="" pause
exit /b 0

:: ------------------------------------------------------------------------------
:: Helpers
:: ------------------------------------------------------------------------------
:kill_existing
tasklist /FI "IMAGENAME eq rusty-grid.exe" 2>NUL | find /I /N "rusty-grid.exe">NUL
if "%ERRORLEVEL%"=="0" (
    taskkill /F /T /IM rusty-grid.exe >nul 2>&1
    timeout /t 1 /nobreak >nul
)
exit /b 0

:detect_role
netstat -ano 2>nul | findstr ":8088" | findstr "LISTENING" >nul
if "%ERRORLEVEL%"=="0" (
    set "CURRENT_ROLE=MASTER (Dang lang nghe tai :8088, Web UI :8080)"
    exit /b 0
)
tasklist /FI "IMAGENAME eq rusty-grid.exe" 2>NUL | find /I /N "rusty-grid.exe">NUL
if "%ERRORLEVEL%"=="0" (
    set "CURRENT_ROLE=WORKER (Dang chay ngam, ket noi ve Master)"
    exit /b 0
)
set "CURRENT_ROLE=STOPPED (Chua chay)"
exit /b 0
