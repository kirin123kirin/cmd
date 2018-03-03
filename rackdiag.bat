@echo off
rem pushd %~dp1

set outtype=svg
set outfile=%Temp%\%~n1.%outtype%

if exist %outfile% del %outfile%

%PYTHONPATH%\Scripts\%~n0.exe %~f1 -T%outtype%

if ERRORLEVEL 0 move /Y %~dpn1.%outtype% %outfile% && start %outfile%

