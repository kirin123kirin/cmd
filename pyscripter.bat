@echo off

set PYTHONHOME=%PYTHONPATH%

%APPROOT%\opt\PyScripter\PyScripter.exe --PYTHON36 --PYTHONDLLPATH "%PYTHONPATH%" $*


