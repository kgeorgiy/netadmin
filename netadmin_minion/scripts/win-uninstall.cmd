@echo off
set "NAME=_netadmin-minion"
set "DIR=%SystemRoot%\admin\%NAME%.service"

net session > nul 2>&1 || (
    echo Run in elevated command prompt
    exit /b 1
)

sc stop "%NAME%"
sc delete "%NAME%"
rmdir /s /q "%SystemRoot%\admin\%NAME%.service"
