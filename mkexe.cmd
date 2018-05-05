@echo off
set WDIR=%TEMP%\pyinstallertmp_%~n1
set CDIR=%~dp1
set NAME=%~n1

pyinstaller.exe -y --distpath %CDIR% --specpath %WDIR% --workpath %WDIR% --exclude-module=IPython --exclude-module=cython --exclude-module=scipy --exclude-module=PyQt5 --exclude-module=sphinx --exclude-module=tk --exclude-module=tcl --exclude-module=tkinter --exclude-module=lib2to3 --exclude-module=matplotlib --exclude-module=PIL --exclude-module=zmq --exclude-module=tornado --exclude-module=lxml --exclude-module=markupsafe --exclude-module=cytoolz %1

rd /s /q %CDIR%\__pycache__

mkdir %CDIR%\bin
move /Y %CDIR%\%NAME%\* %CDIR%\bin
rd /s /q %CDIR%\%NAME%

rd /s /q %WDIR%

