@echo off

%~d0
cd %~dp0
cd ..\

start jupyter.exe lab --ip=* --no-browser --LabApp.password='sha1:0d76e4e1d65a:cf38947fa9e22060a488e1e629a764095bf45d18'

