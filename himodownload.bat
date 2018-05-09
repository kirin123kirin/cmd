@echo off
cd %~dp0

set TARGET=\\10.145.120.39\Y

rem robocopy "%TARGET%" "%APPROOT%"

set CPCMD=FastCopy.exe /auto_close /open_window /cmd=update /exclude="python\"

%CPCMD% "%TARGET%\*" /to="%APPROOT%\"
REM %CPCMD% "%TARGET%\usr\local\sakura\*" /to="%APPROOT%\usr\local\sakura\"
REM %CPCMD% "%TARGET%\usr\local\data\*" /to="%APPROOT%\usr\local\data\"
REM %CPCMD% "%TARGET%\build\*.py" /to="%APPROOT%\build\"
REM %CPCMD% "%TARGET%\etc\*" /to="%APPROOT%\etc\"
REM %CPCMD% "%TARGET%\bin\*" /to="%APPROOT%\bin\"
