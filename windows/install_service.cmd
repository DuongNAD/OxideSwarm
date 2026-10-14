@echo off
setlocal EnableExtensions

rem ==============================================================================
rem  OxideSwarm Windows Service 1-Click Installer (auto-elevates, registers in SCM)
rem
rem    install_service.cmd MASTER_ADDR [SECRET_FILE]
rem
rem  MASTER_ADDR  the master's CLUSTER port, HOST:8088 (not the dashboard's 8080).
rem  SECRET_FILE  the master's cluster.secret copied to this PC (scp or USB).
rem               Default: %USERPROFILE%\.oxideswarm\cluster.secret
rem
rem  The service runs as the low-privilege virtual account NT SERVICE\OxideSwarmWorker,
rem  not as SYSTEM: every task the master sends runs with that identity.
rem  For a master on another network use connect_remote.cmd (P2P ticket) instead.
rem ==============================================================================

if "%~1"=="" goto usage
set "OX_INSTALLER=%~dp0..\packaging\windows\install_windows_service.ps1"
set "OX_MASTER=%~1"
set "OX_SECRET=%~2"

echo [INFO] Requesting Administrator elevation to install the Windows Service...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$q = [char]34; $a = '-NoProfile -ExecutionPolicy Bypass -NoExit -File ' + $q + $env:OX_INSTALLER + $q + ' -Master ' + $q + $env:OX_MASTER + $q + ' -Force -NoElevate'; if ($env:OX_SECRET) { $a += ' -SecretFile ' + $q + $env:OX_SECRET + $q }; Start-Process powershell -ArgumentList $a -Verb RunAs"
exit /b 0

:usage
echo Usage: install_service.cmd MASTER_ADDR [SECRET_FILE]
echo Example: install_service.cmd 192.168.1.100:8088 C:\Users\me\cluster.secret
exit /b 1
