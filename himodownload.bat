@echo off
cd %~dp0

set TARGET=\\10.145.120.39\Y

rem robocopy "%TARGET%" "%APPROOT%"

set CPCMD=FastCopy.exe /auto_close /open_window /cmd=update

%CPCMD% "%TARGET%\opt\bin\*" /to="%APPROOT%\bin\"
%CPCMD% "%TARGET%\opt\cmd\*" /to="%APPROOT%\cmd\"
%CPCMD% "%TARGET%\opt\data\*" /to="%APPROOT%\data\"
%CPCMD% "%TARGET%\build\*.py" /to="%APPROOT%\build\"
%CPCMD% "%TARGET%\etc\*" /to="%APPROOT%\etc\"
%CPCMD% "%TARGET%\home\yellow\*" /to="%APPROOT%\home\yellow\"
