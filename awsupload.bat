@echo off
cd %~dp0

"C:\Program Files (x86)\WinSCP\WinSCP.exe" aws /console /script="%APPROOT%\opt\cmd\awsupload.wsp"

call himoupload.bat
