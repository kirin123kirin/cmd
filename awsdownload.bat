@echo off
cd %~dp0

"C:\Program Files (x86)\WinSCP\WinSCP.exe" aws /console /script="%APPROOT%\usr\local\bin\awsdownload.wsp"

start himodownload.bat

