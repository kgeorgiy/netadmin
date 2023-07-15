@echo off
set "BASE=%~dp0.."
set "NAME=_netadmin-minion"
set "DIR=%SystemRoot%\admin\%NAME%.service"


net session > nul 2>&1 || (
    echo Run in elevated command prompt
    exit /b 1
)

if exist "%BASE%\..\__target" (
    set "TARGET=%BASE%\..\__target"
) else (
    set "TARGET=%BASE%\..\target"
)

mkdir "%DIR%"
call :copy "%TARGET%\release\netadmin-minion-winsvc.exe"
call :copy "%BASE%\resources\netadmin-minion.yaml"
call :copy "%BASE%\__keys\netadmin-server.crt"
call :copy "%BASE%\__keys\netadmin-minion.crt"
call :copy "%BASE%\__keys\netadmin-minion.key"

sc create "%NAME%" start= auto ^
    binPath= "%DIR%\netadmin-minion-winsvc.exe --config %DIR%\netadmin-minion.yaml" ^
    || exit /b 1

sc start "%NAME%" ^
    || exit /b 1

exit /b

:copy
    xcopy /y "%~1" "%DIR%\*"
exit /b
