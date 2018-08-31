@echo off

if %~dp1.==.  (
    for /r %%i in (*.exe *.pyd *.dll) do upx -d "%%i"
) else (
    cd %dp1
    for /r %%i in (*.exe *.pyd *.dll) do upx -d "%%i"
)
