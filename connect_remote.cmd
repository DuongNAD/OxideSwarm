@echo off
setlocal EnableExtensions DisableDelayedExpansion
title OxideSwarm 1-Click Remote Worker Pairing (Windows)

rem ============================================================================
rem  connect_remote.cmd - pair this Windows PC with a master on ANOTHER network
rem
rem  You need two things from the master machine:
rem    1. its P2P ticket (the master must run with --p2p)
rem    2. its cluster secret file (%USERPROFILE%\.oxideswarm\cluster.secret there).
rem       A ticket only ADDRESSES the master, so the worker refuses to pair without
rem       the secret. Copy the file over a trusted channel (scp, USB), never chat.
rem
rem  Usage:  connect_remote.cmd [--ticket-file FILE] [--secret-file FILE]
rem                             [--name NAME] [--insecure] [--help]
rem  Without --ticket-file you are asked to paste the ticket. Do NOT pass the JSON
rem  ticket as an argument: cmd.exe splits arguments at commas and strips quotes.
rem ============================================================================

echo ========================================================
echo   OxideSwarm 1-Click Remote Worker Pairing (Windows)
echo ========================================================
echo.
echo Connects this Windows PC as an OxideSwarm worker to a master on another
echo network through native P2P NAT traversal (iroh QUIC + relay).
echo.
echo You need: (1) the master's P2P ticket and (2) its cluster secret file.
echo.

set "TICKET="
set "TICKET_FILE="
set "TICKET_TEMP="
set "SECRET_FILE="
set "WORKER_NAME="
set "INSECURE="

:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--help" goto usage
if /i "%~1"=="-h" goto usage
if /i "%~1"=="/?" goto usage
if /i "%~1"=="--ticket-file" goto arg_ticket_file
if /i "%~1"=="--ticket-file-is-temp" goto arg_ticket_temp
if /i "%~1"=="--secret-file" goto arg_secret_file
if /i "%~1"=="--name" goto arg_name
if /i "%~1"=="--insecure" goto arg_insecure
if not defined TICKET goto arg_ticket_positional
echo [ERROR] Unexpected argument: %~1
exit /b 1

:arg_ticket_file
set "TICKET_FILE=%~2"
shift
shift
goto parse_args

:arg_ticket_temp
set "TICKET_TEMP=1"
shift
goto parse_args

:arg_secret_file
set "SECRET_FILE=%~2"
shift
shift
goto parse_args

:arg_name
set "WORKER_NAME=%~2"
shift
shift
goto parse_args

:arg_insecure
set "INSECURE=1"
shift
goto parse_args

:arg_ticket_positional
set "TICKET=%~1"
shift
goto parse_args

:usage
echo Usage: %~nx0 [--ticket-file FILE] [--secret-file FILE] [--name NAME] [--insecure]
echo.
echo   --ticket-file FILE   File holding the P2P ticket JSON from the master.
echo                        Without it you are asked to paste the ticket.
echo   --secret-file FILE   Cluster secret copied from the master machine.
echo                        Default: OXIDESWARM_SECRET_FILE, then
echo                        %%USERPROFILE%%\.oxideswarm\cluster.secret
echo   --name NAME          Worker name advertised to the master.
echo   --insecure           Pair WITHOUT a cluster secret. Any host that answers as
echo                        the master could then run commands on this PC.
echo.
echo The service runs as a low-privilege virtual account, not as SYSTEM.
exit /b 0

:args_done

rem ---- 1. Ticket: file, positional argument or interactive paste ----------------
if defined TICKET_FILE goto have_ticket_source
if defined TICKET goto have_ticket_source
set /p "TICKET=Paste your OxideSwarm P2P Ticket from Master: "

:have_ticket_source
if defined TICKET_FILE if not exist "%TICKET_FILE%" goto ticket_file_missing
if not defined TICKET_FILE if not defined TICKET goto ticket_missing

rem Validate and normalise through PowerShell, reading the ticket from an environment
rem variable or file so that its JSON quotes never pass through cmd.exe parsing.
set "OX_TICKET=%TICKET%"
set "OX_TICKET_IN=%TICKET_FILE%"
set "OX_TICKET_OUT=%TEMP%\oxideswarm-ticket-%RANDOM%%RANDOM%.txt"
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { if ($env:OX_TICKET_IN) { $t = Get-Content -LiteralPath $env:OX_TICKET_IN -Raw } else { $t = $env:OX_TICKET }; $t = $t.Trim().TrimStart([char]0xFEFF).Trim(); $o = ConvertFrom-Json -InputObject $t; if (-not $o.id) { exit 3 }; if ($t.Contains([string][char]39)) { exit 4 }; [IO.File]::WriteAllText($env:OX_TICKET_OUT, $t) } catch { exit 3 }"
set "OX_TICKET="
set "OX_TICKET_IN="
if errorlevel 4 goto ticket_quote
if errorlevel 1 goto ticket_invalid
echo [OK] Validated P2P Ticket format.
if defined TICKET_TEMP if defined TICKET_FILE del "%TICKET_FILE%" >nul 2>&1

rem ---- 2. Cluster secret ----------------------------------------------------
if not defined SECRET_FILE if defined OXIDESWARM_SECRET_FILE set "SECRET_FILE=%OXIDESWARM_SECRET_FILE%"
if not defined SECRET_FILE if exist "%USERPROFILE%\.oxideswarm\cluster.secret" set "SECRET_FILE=%USERPROFILE%\.oxideswarm\cluster.secret"
if defined SECRET_FILE if not exist "%SECRET_FILE%" goto secret_missing
if not defined SECRET_FILE if not defined INSECURE goto secret_missing
if defined SECRET_FILE echo [OK] Cluster secret: %SECRET_FILE%
if defined INSECURE echo [WARN] --insecure: this worker will NOT authenticate the master.

rem Arguments handed to the installer / to the elevated copy of this script.
set "OX_EXTRA_ARGS="
if defined SECRET_FILE set "OX_EXTRA_ARGS=%OX_EXTRA_ARGS% --secret-file "%SECRET_FILE%""
if defined WORKER_NAME set "OX_EXTRA_ARGS=%OX_EXTRA_ARGS% --name "%WORKER_NAME%""
if defined INSECURE set "OX_EXTRA_ARGS=%OX_EXTRA_ARGS% --insecure"

rem ---- 3. Administrator privilege verification and self-elevation -------------
net session >nul 2>&1
if %errorlevel% equ 0 goto is_admin
echo.
echo [INFO] Requesting Administrator privileges to register the Windows Background Service...
set "OX_SELF=%~f0"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$q = [char]34; $a = '/c ' + $q + $q + $env:OX_SELF + $q + ' --ticket-file-is-temp --ticket-file ' + $q + $env:OX_TICKET_OUT + $q + $env:OX_EXTRA_ARGS + $q; Start-Process -FilePath cmd.exe -ArgumentList $a -Verb RunAs"
exit /b 0

:is_admin
rem ---- 4. Locate and execute the Windows service installer --------------------
set "SCRIPT_DIR=%~dp0"
set "INSTALLER=%SCRIPT_DIR%packaging\windows\install_windows_service.ps1"

set "INSTALL_ARGS="
if defined SECRET_FILE set "INSTALL_ARGS=%INSTALL_ARGS% -SecretFile "%SECRET_FILE%""
if defined WORKER_NAME set "INSTALL_ARGS=%INSTALL_ARGS% -WorkerName "%WORKER_NAME%""
if defined INSECURE set "INSTALL_ARGS=%INSTALL_ARGS% -Insecure"

if not exist "%INSTALLER%" goto installer_missing
echo.
echo [INFO] Installing OxideSwarm as a native Windows Background Service...
rem -Force replaces an already installed service (re-pair or upgrade). The service
rem runs as a low-privilege virtual account, never as SYSTEM.
powershell -NoProfile -ExecutionPolicy Bypass -File "%INSTALLER%" -P2pTicketFile "%OX_TICKET_OUT%" -Force -NoElevate%INSTALL_ARGS%
set "PS_EXIT=%errorlevel%"
if not "%PS_EXIT%"=="0" goto installer_failed

echo.
echo ========================================================
echo [OK] OxideSwarm Worker service installed.
echo - Service Name  : OxideSwarmWorker (runs as NT SERVICE\OxideSwarmWorker)
echo - Startup Type  : Automatic, starts on boot
echo - NAT Traversal : native iroh QUIC + relay
echo - Reconnect     : automatic, with backoff
echo.
echo Confirm that it really joined the grid: the installer waited for
echo "Registration accepted" in C:\ProgramData\OxideSwarm\logs\worker.out.log
echo and warned above if it did not appear. You can re-check with:
echo   powershell -ExecutionPolicy Bypass -File "%SCRIPT_DIR%packaging\windows\verify_windows_service.ps1"
echo ========================================================
del "%OX_TICKET_OUT%" >nul 2>&1
echo.
pause
exit /b 0

:installer_missing
echo.
echo [WARN] Windows Service installer not found at %INSTALLER%.
goto FALLBACK_FOREGROUND

:installer_failed
echo.
echo [WARN] Windows Service registration failed with exit code %PS_EXIT%.
goto FALLBACK_FOREGROUND

:FALLBACK_FOREGROUND
echo [INFO] Launching worker directly in foreground mode...
set "BIN="
if exist "%SCRIPT_DIR%rusty-grid.exe" set "BIN=%SCRIPT_DIR%rusty-grid.exe"
if exist "%SCRIPT_DIR%target\release\rusty-grid.exe" set "BIN=%SCRIPT_DIR%target\release\rusty-grid.exe"
if exist "%SCRIPT_DIR%target\debug\rusty-grid.exe" set "BIN=%SCRIPT_DIR%target\debug\rusty-grid.exe"
if not defined BIN goto no_binary

rem The ticket and secret path travel as environment variables of the child process
rem (RUSTY_GRID_P2P_TICKET / OXIDESWARM_SECRET_FILE): nothing secret on the command line.
set /p RUSTY_GRID_P2P_TICKET=<"%OX_TICKET_OUT%"
if defined SECRET_FILE set "OXIDESWARM_SECRET_FILE=%SECRET_FILE%"
set "WORKER_FLAGS="
if defined WORKER_NAME set "WORKER_FLAGS=%WORKER_FLAGS% --name "%WORKER_NAME%""
if defined INSECURE set "WORKER_FLAGS=%WORKER_FLAGS% --insecure"
echo Starting worker process: %BIN%
"%BIN%" worker%WORKER_FLAGS%
del "%OX_TICKET_OUT%" >nul 2>&1
echo.
pause
exit /b 1

:no_binary
echo [ERROR] Could not locate rusty-grid.exe binary.
echo Please compile via 'cargo build --release --bin rusty-grid' or place rusty-grid.exe in this folder.
del "%OX_TICKET_OUT%" >nul 2>&1
echo.
pause
exit /b 1

:ticket_missing
echo [ERROR] No P2P ticket provided. Exiting.
pause
exit /b 1

:ticket_file_missing
echo [ERROR] Ticket file not found: %TICKET_FILE%
pause
exit /b 1

:ticket_quote
echo [ERROR] The ticket contains a single quote and cannot be stored in the configuration.
del "%OX_TICKET_OUT%" >nul 2>&1
pause
exit /b 1

:ticket_invalid
echo [ERROR] Invalid ticket! Expected the JSON from the master containing an "id" field.
echo If you passed it as an argument, cmd.exe splits arguments at commas and removes
echo quotes: use --ticket-file FILE, or run without arguments and paste it at the prompt.
del "%OX_TICKET_OUT%" >nul 2>&1
pause
exit /b 1

:secret_missing
echo.
echo [ERROR] No cluster secret found.
echo A P2P ticket only says WHERE the master is; the cluster secret is what proves it is
echo the real one. Copy the file from the master machine over a trusted channel, for example:
echo   scp user@master:~/.oxideswarm/cluster.secret "%USERPROFILE%\.oxideswarm\cluster.secret"
echo or carry it on a USB stick (never paste it into chat or email), then run this script
echo again or pass --secret-file FILE. To pair WITHOUT authenticating the master, pass --insecure.
del "%OX_TICKET_OUT%" >nul 2>&1
pause
exit /b 1
