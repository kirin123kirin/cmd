
set WDIR=%TEMP%\pythonsrc
set VERSION=%1

if "%VERSION%" == "" (
  echo "引数がありません。アップデートしたいバージョンを指定してください。"
  echo "例: pythonupgrade.bat 3.7.5"
  pause
  exit /b 1
)

set VNUM=%VERSION:.=%

rd /s /q %WDIR%
mkdir %WDIR%
cd %WDIR%
C:

set PYTHONORG=https://www.python.org/ftp/python/%VERSION%/amd64
set MYREPO=https://raw.githubusercontent.com/kirin123kirin/cmd/master
set MYDOTFILE=https://raw.githubusercontent.com/kirin123kirin/dotfile/master/python
set WGETCMD=wget --no-check-certificate

%WGETCMD% %PYTHONORG%/core.msi
%WGETCMD% %PYTHONORG%/dev.msi
%WGETCMD% %PYTHONORG%/doc.msi
%WGETCMD% %PYTHONORG%/exe.msi
%WGETCMD% %PYTHONORG%/launcher.msi
%WGETCMD% %PYTHONORG%/lib.msi
%WGETCMD% %PYTHONORG%/path.msi
%WGETCMD% %PYTHONORG%/test.msi
%WGETCMD% %PYTHONORG%/pip.msi
%WGETCMD% %PYTHONORG%/tcltk.msi
%WGETCMD% %PYTHONORG%/tools.msi
%WGETCMD% %PYTHONORG%/ucrt.msi

mkdir %WDIR%\python
msiexec /a core.msi targetdir=%WDIR%\python /qn
msiexec /a dev.msi targetdir=%WDIR%\python /qn
msiexec /a doc.msi targetdir=%WDIR%\python /qn
msiexec /a exe.msi targetdir=%WDIR%\python /qn
msiexec /a launcher.msi targetdir=%WDIR%\python /qn
msiexec /a lib.msi targetdir=%WDIR%\python /qn
msiexec /a path.msi targetdir=%WDIR%\python /qn
msiexec /a test.msi targetdir=%WDIR%\python /qn
msiexec /a pip.msi targetdir=%WDIR%\python /qn
msiexec /a tcltk.msi targetdir=%WDIR%\python /qn
msiexec /a tools.msi targetdir=%WDIR%\python /qn
msiexec /a ucrt.msi targetdir=%WDIR%\python /qn

del python\*.msi
del *.msi
del python\py.exe
del python\pyw.exe
del python\pyshellext*.dll
del python\NEWS.txt

rem %ProgramFiles%\api-ms-win-*.dll
move python\api-*.dll python\DLLs\
move python\ucrtbase.dll python\DLLs\

cd python
%WGETCMD% %MYDOTFILE%/python37.pth -O python%VNUM:~,2%.pth
rem copy Y:\usr\local\python\Lib\site-packages\custom.pth python\Lib\site-packages\
REM rd /s /q Y:\usr\local\python
REM move python Y:\usr\local\

%WGETCMD% https://raw.githubusercontent.com/pypa/get-pip/master/get-pip.py
.\python.exe get-pip.py
del get-pip.py

set PIP=%WDIR%\python\Scripts\pip.exe
rem %PIP% install -r %MYDOTFILE%/requirements.txt
%WGETCMD% %MYDOTFILE%/requirements.txt -qO - | grep -v "^#" | grep -v "spyder" > requirement.txt
%PIP% install -r requirement.txt

%PIP% install -r %MYDOTFILE%/requirements_win.txt

%WGETCMD% %MYREPO%/repair_scripts.py
%WGETCMD% %MYREPO%/install.bat

cd ..
7z.exe a python%VNUM%.zip python
cd python

%PIP% install -r %VNUM%/requirements.txt
cd ..
7z.exe a python%VNUM%_full.zip python

