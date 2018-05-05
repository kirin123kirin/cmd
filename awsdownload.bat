@echo off
cd %~dp0

"C:\Program Files (x86)\WinSCP\WinSCP.exe" aws /console /script="%APPROOT%\cmd\awsdownload.wsp"

call himodownload.bat

