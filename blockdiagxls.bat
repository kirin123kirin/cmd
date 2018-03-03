@echo off
rem pushd %~dp1

set outfile=%Temp%\%~n1.xls
set orgcommand=%~n0
set command=%orgcommand:xls=%

if exist %outfile% del %outfile%

%PYTHONPATH%\Scripts\%command%.exe -T shape.xls %~f1
if ERRORLEVEL 0 move /Y %~n1.shape.xls %outfile% && start %outfile%

