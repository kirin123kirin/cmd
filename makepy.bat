@echo off
set TARGET=%~df1
C:
cd %TEMP%
mkdir nuitka.tmp
cd nuitka.tmp
nuitka3 --mingw --recurse-stdlibq %TARGET%

start .
