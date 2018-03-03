@echo off
cd %~dp0

set TARGET=\\10.145.120.39\Y

rem robocopy "%TARGET%" "%APPROOT%"

set CPCMD=FastCopy.exe /auto_close /open_window /cmd=update

%CPCMD% "%APPROOT%\opt\bin\*" /to="%TARGET%\opt\bin\"
%CPCMD% "%APPROOT%\opt\cmd\*" /to="%TARGET%\opt\cmd\"
%CPCMD% "%APPROOT%\opt\data\*" /to="%TARGET%\opt\data\"
%CPCMD% "%APPROOT%\build\*.py" /to="%TARGET%\build\"
%CPCMD% "%APPROOT%\etc\*" /to="%TARGET%\etc\"
%CPCMD% "%APPROOT%\usr\*" /to="%TARGET%\usr\"
%CPCMD% "%APPROOT%\*.bat" /to="%TARGET%\"
%CPCMD% "%APPROOT%\*.cmd" /to="%TARGET%\"
%CPCMD% "%APPROOT%\*.ini" /to="%TARGET%\"
